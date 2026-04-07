#!/usr/bin/env python3
"""
Generate a local conda channel from PyPI metadata.

Only the PyPI HTTP fetch and ``packages.txt`` parsing live here; all channel
artifacts (``repodata.json``, ``repodata_from_packages.json``, ``.zst``,
``noarch/index.html``, ``channeldata.json``, root ``index.html``) are produced
by conda-index.
"""

from __future__ import annotations

import argparse
import asyncio
import time
from pathlib import Path
from typing import Any

import httpx
from conda_index.index import ChannelIndex
from conda_index.utils import CONDA_PACKAGE_EXTENSIONS

from conda_pypi.markers import pypi_to_repodata_noarch_whl_entry

SUBDIR = "noarch"


async def get_repodata_entry(
    name: str, version: str, client: httpx.AsyncClient, max_retries: int = 3
) -> dict[str, Any] | None:
    pypi_endpoint = f"https://pypi.org/pypi/{name}/{version}/json"

    for attempt in range(max_retries):
        try:
            response = await client.get(
                pypi_endpoint,
                headers={
                    "Accept": "application/json",
                    "Accept-Encoding": "gzip, deflate, br",
                },
            )
            response.raise_for_status()
            pypi_data = response.json()
            if not pypi_data:
                return None
            return pypi_to_repodata_noarch_whl_entry(pypi_data)
        except httpx.HTTPStatusError as e:
            if attempt < max_retries - 1:
                wait_time = 2**attempt
                print(
                    f"  ⚠️  Error fetching {name} {version} (attempt {attempt + 1}/{max_retries}): {e}"
                )
                print(f"     Retrying in {wait_time}s...")
                await asyncio.sleep(wait_time)
            else:
                print(
                    f"  ❌ Error fetching {name} {version} after {max_retries} attempts: {e}"
                )
                return None
        except Exception as e:
            print(f"  ❌ Error processing {name} {version}: {e}")
            return None

    return None


def parse_packages_file(filepath: Path) -> list[tuple[str, str]]:
    packages: list[tuple[str, str]] = []
    with open(filepath) as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "==" in line:
                name, version = line.split("==", 1)
                packages.append((name.strip(), version.strip()))
            else:
                print(f"  ⚠️  Missing version on line {line_num}: {line}")
                print("      Expected format: package-name==version")
    return packages


def index_wheel_entries_with_conda_index(
    channel_root: Path, entries: list[dict[str, Any]]
) -> None:
    """
    Push wheel ``index.json``-shaped dicts through the cache and run a full
    conda-index pass (repodata v3, channeldata, HTML indexes).
    """
    (channel_root / SUBDIR).mkdir(parents=True, exist_ok=True)

    channel_index = ChannelIndex(
        channel_root,
        channel_root.name,
        subdirs=[SUBDIR],
        repodata_v3=True,
        update_only=False,
        save_fs_state=False,
        write_current_repodata=False,
        write_zst=True,
        compact_json=False,
        cache_kwargs={"package_extensions": CONDA_PACKAGE_EXTENSIONS + (".whl",)},
    )
    cache = channel_index.cache_for_subdir(SUBDIR)

    stated = [(rec, int(rec.get("timestamp", 1))) for rec in entries]

    cache.store_fs_state(
        (
            {
                "path": cache.database_path(rec["fn"]),
                "mtime": ts,
                "size": rec["size"],
            }
            for rec, ts in stated
        )
    )

    for rec, ts in stated:
        idx = dict(rec)
        idx["md5"] = None  # conda-index requires key; sha256 comes from PyPI
        assert "sha256" in idx and "fn" in idx
        cache.store(
            cache.database_path(rec["fn"]),
            idx["size"],
            ts,
            {},
            idx,
        )

    channel_index.index(patch_generator=None)
    channel_index.update_channeldata(rss=False)


async def fetch_entries_from_pypi(
    packages: list[tuple[str, str]],
    *,
    concurrency: int,
) -> tuple[list[dict[str, Any]], list[str]]:
    entries: list[dict[str, Any]] = []
    failed: list[str] = []

    client = httpx.AsyncClient(
        http2=True,
        limits=httpx.Limits(
            max_connections=concurrency,
            max_keepalive_connections=50,
            keepalive_expiry=60.0,
        ),
        timeout=httpx.Timeout(30.0, connect=10.0),
    )
    semaphore = asyncio.Semaphore(concurrency)

    async def fetch_one(
        pkg_tuple: tuple[str, str],
    ) -> tuple[tuple[str, str], dict[str, Any] | None]:
        async with semaphore:
            n, v = pkg_tuple
            return (pkg_tuple, await get_repodata_entry(n, v, client))

    try:
        completed = 0
        start_time = time.perf_counter()
        results = await asyncio.gather(
            *[fetch_one(t) for t in packages],
            return_exceptions=True,
        )

        for result in results:
            if isinstance(result, Exception):
                completed += 1
                continue
            pkg_tuple, entry = result
            name, version = pkg_tuple
            completed += 1
            elapsed = time.perf_counter() - start_time
            rate = completed / elapsed if elapsed > 0 else 0
            if entry:
                entries.append(entry)
                if completed % 100 == 0 or completed == len(packages):
                    print(
                        f"  ✅ [{completed}/{len(packages)}] {name} {version} ({rate:.1f}/s)"
                    )
            else:
                failed.append(f"{name}=={version}")
                print(
                    f"  ⚠️  [{completed}/{len(packages)}] {name} {version} - no wheel found"
                )
    finally:
        await client.aclose()

    return entries, failed


async def main_async(concurrency: int = 100, packages_file: Path | None = None) -> int:
    repo_root = Path(__file__).parent.resolve()
    if packages_file is None:
        packages_file = repo_root / "packages.txt"

    if not packages_file.exists():
        print("❌ Error: packages.txt not found!")
        return 1

    print("📋 Reading packages list...")
    packages = parse_packages_file(packages_file)
    if not packages:
        print("⚠️  No valid packages found")
        return 1

    print(f"📍 Channel root: {repo_root}")
    print(f"🚀 Concurrency: {concurrency}\n")

    print(f"📦 Fetching {len(packages)} packages with async HTTP/2...\n")
    entries, failed = await fetch_entries_from_pypi(
        packages, concurrency=concurrency
    )

    if failed:
        print(f"\n⚠️  WARNING: {len(failed)} package(s) missing wheels:\n")
        for pkg in sorted(failed):
            print(f"   - {pkg}")
        print("\nThese packages were skipped and not included in repodata.json\n")

    index_wheel_entries_with_conda_index(repo_root, entries)

    out_json = repo_root / SUBDIR / "repodata.json"
    print(f"\n✨ conda-index wrote {len(entries)} wheels → {out_json}")
    zst = repo_root / SUBDIR / "repodata.json.zst"
    if zst.exists():
        print(f"   (+ {zst.name})")
    print(f"   (+ channeldata.json, index.html under {repo_root})")

    print("\n✅ Done! Run: python -m http.server 8000\n")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build a conda channel from PyPI (conda-index writes all channel files)"
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=100,
        help="Concurrent PyPI requests (default: 100)",
    )
    parser.add_argument(
        "--packages-file",
        type=Path,
        default=None,
        metavar="FILE",
        help="Package list (default: packages.txt next to this script)",
    )
    args = parser.parse_args()
    return asyncio.run(main_async(args.concurrency, args.packages_file))


if __name__ == "__main__":
    exit(main())
