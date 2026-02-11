#!/usr/bin/env python3
"""
Discover PyPI packages with wheels for conda-pypi-test.

Two modes:
  1. --names-only   Fetch all package names from PyPI Simple Index.
  2. --from-names   Read names from a file; fetch latest version from PyPI
                    and keep only packages that have a wheel.

Usage:
  python discover.py --names-only --output names.txt
  python discover.py --from-names names.txt --output packages.txt
"""

import argparse
import json
import re
import sys
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, List, Optional, Tuple

import httpx

PYPI_SIMPLE_INDEX_URL = "https://pypi.org/simple/"
PYPI_JSON_URL = "https://pypi.org/pypi"

_thread_local = threading.local()


def normalize_package_name(name: str) -> str:
    """Normalize package name (lowercase, replace _ with -)."""
    return name.lower().replace("_", "-")


def fetch_package_names_pypi_simple(timeout: int = 120) -> List[str]:
    """Fetch all PyPI package names from the Simple Index (PEP 503)."""
    req = urllib.request.Request(
        PYPI_SIMPLE_INDEX_URL,
        headers={"Accept": "application/vnd.pypi.simple.v1+json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode()
    try:
        data = json.loads(body)
        projects = data.get("projects", [])
        names = [normalize_package_name(p["name"]) for p in projects if p.get("name")]
    except json.JSONDecodeError:
        names = re.findall(r'href="[^"]*/simple/([^/"]+)/"', body)
        names = [normalize_package_name(n) for n in names]
    return sorted(set(names))


def _version_sort_key(v: str) -> Tuple[int, ...]:
    """Sort key for version strings (numeric prefix only)."""
    m = re.match(r"^(\d+(?:\.\d+)*)", str(v))
    if not m:
        return (0,)
    return tuple(int(x) for x in m.group(1).split("."))


def _fetch_simple_page(name: str, client: Any, timeout: int) -> Optional[str]:
    """Fetch /simple/<name>/ page; return HTML body or None."""
    url = f"{PYPI_SIMPLE_INDEX_URL}{name}/"
    try:
        if client is not None:
            r = client.get(url, timeout=timeout, headers={"Accept": "text/html"})
            r.raise_for_status()
            return r.text
        req = urllib.request.Request(url, headers={"Accept": "text/html"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode()
    except Exception:
        return None


def _parse_simple_page_for_wheels(html: str, pure_only: bool) -> Optional[Tuple[str, bool]]:
    """
    Parse simple index HTML for wheel filenames. Return (latest_version, has_required_wheel) or None.
    Wheel filename format: name-version-{py}-{abi}-{platform}.whl
    """
    whl_filenames = re.findall(
        r"([a-zA-Z0-9_.-]+-\d+[\w.]*-[a-zA-Z0-9_.-]+-[a-zA-Z0-9_.-]+-[a-zA-Z0-9_.-]+\.whl)",
        html,
    )
    if not whl_filenames:
        return None
    # Group by version: version -> (has_pure, has_any)
    by_version: dict[str, Tuple[bool, bool]] = {}
    for fn in set(whl_filenames):
        parts = fn[:-4].split("-")
        if len(parts) < 5:
            continue
        version = parts[-4]
        abi, platform = parts[-2], parts[-1]
        has_pure = abi == "none" and platform == "any"
        p, a = by_version.get(version, (False, False))
        by_version[version] = (p or has_pure, True)
    if not by_version:
        return None
    best_version = max(by_version.keys(), key=_version_sort_key)
    has_pure, has_any = by_version[best_version]
    ok = has_pure if pure_only else has_any
    return (best_version, ok) if ok else None


def fetch_package_version_and_wheels(
    name: str,
    pure_only: bool = True,
    timeout: int = 30,
    client: Any = None,
    use_simple_first: bool = True,
) -> Optional[Tuple[str, bool]]:
    """
    Fetch latest version and wheel availability for one package from PyPI.
    client: httpx.Client for connection reuse (and Simple API); None to use urllib.
    Returns (version, ok) or None.
    """
    if use_simple_first and client is not None:
        html = _fetch_simple_page(name, client, timeout)
        if html:
            out = _parse_simple_page_for_wheels(html, pure_only)
            if out is not None:
                return out
    # Fallback: full JSON API (has definitive latest version and file list)
    url = f"{PYPI_JSON_URL}/{name}/json"
    try:
        if client is not None:
            r = client.get(url, timeout=timeout, headers={"Accept": "application/json"})
            r.raise_for_status()
            data = r.json()
        else:
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = json.loads(resp.read().decode())
    except Exception:
        return None
    info = data.get("info", {})
    version = info.get("version")
    if not version:
        return None
    urls = data.get("urls", [])
    if not urls:
        urls = data.get("releases", {}).get(version, [])
    has_pure = False
    has_any_wheel = False
    for f in urls:
        if f.get("packagetype") != "bdist_wheel":
            continue
        has_any_wheel = True
        if (f.get("filename") or "").endswith("none-any.whl"):
            has_pure = True
            break
    ok = has_pure if pure_only else has_any_wheel
    return (version, ok) if ok else None


def _init_worker_client() -> None:
    """Initialize httpx client for this worker (HTTP/2 when h2 is available)."""
    try:
        _thread_local.client = httpx.Client(http2=True)
    except Exception:
        _thread_local.client = httpx.Client(http2=False)


def _fetch_one(name: str, pure_only: bool, timeout: int) -> Optional[Tuple[str, bool]]:
    """One package lookup using this worker's HTTP client."""
    client = getattr(_thread_local, "client", None)
    return fetch_package_version_and_wheels(
        name,
        pure_only=pure_only,
        timeout=timeout,
        client=client,
        use_simple_first=client is not None,
    )


def discover_from_names_file(
    names_path: Path,
    pure_only: bool = True,
    workers: int = 50,
    delay: float = 0.0,
    timeout: int = 30,
) -> List[Tuple[str, str]]:
    """Read names from file; for each, fetch PyPI metadata and return (name, version) for packages with wheels."""
    names = []
    with open(names_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            names.append(line)
    if not names:
        return []
    results: List[Tuple[str, str]] = []
    done = 0
    start = time.perf_counter()
    init = _init_worker_client
    with ThreadPoolExecutor(max_workers=workers, initializer=init) as executor:
        futures = {
            executor.submit(_fetch_one, name, pure_only, timeout): name
            for name in names
        }
        for future in as_completed(futures):
            name = futures[future]
            done += 1
            if done % 5000 == 0 or done == len(names):
                elapsed = time.perf_counter() - start
                rate = done / elapsed if elapsed > 0 else 0
                print(f"   Checked {done:,}/{len(names):,} ({rate:.0f}/s)...")
            try:
                out = future.result()
                if out is not None:
                    version, _ = out
                    results.append((name, version))
            except Exception:
                pass
            if delay > 0:
                time.sleep(delay / workers)
    return results


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Discover PyPI packages with wheels (names only, or from names file with version + wheel filter)",
    )
    parser.add_argument("--output", type=Path, default=Path("packages.txt"), help="Output file")
    parser.add_argument("--names-only", action="store_true", help="Fetch all package names from PyPI Simple Index")
    parser.add_argument("--from-names", type=Path, metavar="FILE", help="Read names from FILE; keep only packages with wheels")
    parser.add_argument("--any-wheel", action="store_true", help="With --from-names: keep any wheel (default: pure Python only)")
    parser.add_argument("--workers", type=int, default=50, help="With --from-names: concurrent requests (default 50; reduce if rate limited)")
    parser.add_argument("--delay", type=float, default=0.0, help="With --from-names: extra delay per completion in seconds (default 0)")
    args = parser.parse_args()

    if args.from_names is not None:
        if not args.from_names.exists():
            print(f"❌ File not found: {args.from_names}")
            sys.exit(1)
        pure_only = not args.any_wheel
        print(f"📋 Reading names from {args.from_names}...")
        print(f"   Filter: {'pure Python wheel only' if pure_only else 'any wheel'}")
        results = discover_from_names_file(
            args.from_names,
            pure_only=pure_only,
            workers=args.workers,
            delay=args.delay,
        )
        args.output.parent.mkdir(parents=True, exist_ok=True)
        lines = ["# Format: package-name==version", ""] + [f"{n}=={v}" for n, v in sorted(results)]
        args.output.write_text("\n".join(lines) + "\n")
        print(f"\n✅ Saved {len(results):,} packages to {args.output}")
        return

    if args.names_only:
        print("📋 Fetching package names from PyPI Simple Index...")
        try:
            names = fetch_package_names_pypi_simple()
        except Exception as e:
            print(f"❌ Error: {e}")
            sys.exit(1)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text("\n".join(names) + "\n")
        print(f"✅ Saved {len(names):,} package names to {args.output}")
        return

    print("Use --names-only or --from-names FILE. See --help.")
    sys.exit(1)


if __name__ == "__main__":
    main()
