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
from unearth import PackageFinder

PYPI_SIMPLE_INDEX_URL = "https://pypi.org/simple/"

_thread_local = threading.local()


def normalize_package_name(name: str) -> str:
    """Normalize package name (lowercase, replace _ with -)."""
    return name.lower().replace("_", "-")


def fetch_package_names_pypi_simple(timeout: int = 120) -> list[str]:
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


def fetch_package_version_and_wheels(
    name: str,
    pure_only: bool = True,
    timeout: int = 30,
    finder: PackageFinder | None = None,
) -> tuple[str, bool] | None:
    """
    Fetch latest version and wheel availability for one package from PyPI.
    Uses unearth's PackageFinder to select best matching candidates.
    Returns (version, ok) or None.
    """
    del timeout
    if finder is None:
        finder = PackageFinder(index_urls=[PYPI_SIMPLE_INDEX_URL])
    try:
        packages = finder.find_all_packages(name)
    except Exception:
        return None
    for package in packages:
        try:
            pkg_json = package.as_json()
        except Exception:
            pkg_json = {}
        version = pkg_json.get("version") if isinstance(pkg_json, dict) else None
        link = pkg_json.get("link") if isinstance(pkg_json, dict) else None
        url = link.get("url") if isinstance(link, dict) else None
        if not url:
            url = getattr(getattr(package, "link", None), "url", "")
        if not isinstance(url, str) or not url.endswith(".whl"):
            continue
        if pure_only and not url.endswith("none-any.whl"):
            continue
        if not version:
            version = getattr(package, "version", None)
        if not version:
            continue
        return (str(version), True)
    return None


def _init_worker_client() -> None:
    """Initialize PackageFinder for this worker."""
    _thread_local.finder = PackageFinder(index_urls=[PYPI_SIMPLE_INDEX_URL])


def _fetch_one(name: str, pure_only: bool, timeout: int) -> tuple[str, bool] | None:
    """One package lookup using this worker's finder."""
    finder = getattr(_thread_local, "finder", None)
    return fetch_package_version_and_wheels(
        name,
        pure_only=pure_only,
        timeout=timeout,
        finder=finder,
    )


def discover_from_names_file(
    names_path: Path,
    pure_only: bool = True,
    workers: int = 50,
    delay: float = 0.0,
    timeout: int = 30,
) -> list[tuple[str, str]]:
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
    results: list[tuple[str, str]] = []
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
