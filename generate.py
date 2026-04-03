#!/usr/bin/env python3
"""
Generate conda repodata.json from PyPI packages.

Fetches package metadata from PyPI using async HTTP/2 for fast performance.

Usage:
    python generate.py [--concurrency N]
"""

import argparse
import asyncio
import json
try:
    from compression.zstd import compress as zstd_compress  # Python 3.14+
except ImportError:
    from backports.zstd import compress as zstd_compress  # type: ignore[no-redef]
import httpx
import time
from pathlib import Path
from typing import Any

from conda_pypi.markers import pypi_to_repodata_noarch_whl_entry


async def get_repodata_entry(
    name: str, version: str, client: httpx.AsyncClient, max_retries: int = 3
) -> dict[str, Any] | None:
    """
    Fetch package data from PyPI and convert to repodata entry.

    Args:
        name: Package name
        version: Package version
        client: Async HTTP client
        max_retries: Maximum number of retry attempts

    Returns:
        Repodata entry dictionary or None if failed
    """
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
                wait_time = 2**attempt  # Exponential backoff: 1s, 2s, 4s
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
    """
    Parse packages.txt file into list of (name, version) tuples.

    Format (one package per line):

    - ``name==version`` — PyPI distribution name and version. Only the first ``==``
      splits name and version (so versions may contain ``==`` if needed).
    - Any other non-empty line is skipped with a warning (no ``==``).

    Args:
        filepath: Path to packages.txt file

    Returns:
        List of (name, version) tuples
    """
    packages = []

    with open(filepath) as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()

            # Skip empty lines and comments
            if not line or line.startswith("#"):
                continue

            if "==" in line:
                name, version = line.split("==", 1)
                packages.append((name.strip(), version.strip()))
            else:
                print(f"  ⚠️  Missing version on line {line_num}: {line}")
                print(f"      Expected format: package-name==version")

    return packages


async def generate_repodata(
    packages: list[tuple[str, str]], output_dir: Path, concurrency: int = 100
) -> dict[str, Any]:
    """
    Generate repodata.json from list of packages using async HTTP/2.

    Args:
        packages: List of (name, version) tuples
        output_dir: Directory to write repodata.json
        concurrency: Maximum number of concurrent requests

    Returns:
        Generated repodata dictionary
    """
    pkg_whls = {}
    failed_packages = []

    print(f"📦 Fetching {len(packages)} packages with async HTTP/2...\n")

    client = httpx.AsyncClient(
        http2=True,
        limits=httpx.Limits(
            max_connections=concurrency,
            max_keepalive_connections=50,
            keepalive_expiry=60.0,
        ),
        timeout=httpx.Timeout(30.0, connect=10.0),
    )

    try:
        completed = 0
        start_time = time.perf_counter()

        # Process in batches to avoid overwhelming the system
        batch_size = concurrency * 10

        for i in range(0, len(packages), batch_size):
            batch = packages[i : i + batch_size]

            # Create semaphore to limit concurrent requests
            semaphore = asyncio.Semaphore(concurrency)

            async def fetch_with_semaphore(
                pkg_tuple: tuple[str, str],
            ) -> tuple[tuple[str, str], dict[str, Any] | None]:
                async with semaphore:
                    name, version = pkg_tuple
                    result = await get_repodata_entry(name, version, client)
                    return (pkg_tuple, result)

            # Process batch concurrently
            tasks = [fetch_with_semaphore(pkg_tuple) for pkg_tuple in batch]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)

            # Collect results
            for result in batch_results:
                if isinstance(result, Exception):
                    completed += 1
                    continue

                pkg_tuple, entry = result
                name, version = pkg_tuple
                completed += 1

                # Calculate rate
                elapsed = time.perf_counter() - start_time
                rate = completed / elapsed if elapsed > 0 else 0

                if entry:
                    pkg_whls[f"{entry['name']}-{version}-py3_none_any_0"] = entry

                    # Show progress every 100 packages or on completion
                    if completed % 100 == 0 or completed == len(packages):
                        print(
                            f"  ✅ [{completed}/{len(packages)}] {name} {version} ({rate:.1f}/s)"
                        )
                else:
                    failed_packages.append(f"{name}=={version}")
                    print(
                        f"  ⚠️  [{completed}/{len(packages)}] {name} {version} - no wheel found"
                    )

    finally:
        await client.aclose()

    # Check if any packages failed
    if failed_packages:
        print(f"\n⚠️  WARNING: {len(failed_packages)} package(s) missing wheels:\n")
        for pkg in sorted(failed_packages):
            print(f"   - {pkg}")
        print(f"\nThese packages were skipped and not included in repodata.json\n")

    repodata_output = {
        "info": {"subdir": "noarch"},
        "packages": {},
        "packages.conda": {},
        "removed": [],
        "repodata_version": 3,
        "signatures": {},
        "v3": {"whl": dict(sorted(pkg_whls.items()))},
    }

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Serialize JSON once
    json_data = json.dumps(repodata_output, indent=2)
    json_bytes = json_data.encode("utf-8")

    # Write uncompressed JSON
    output_file = output_dir / "repodata.json"
    with open(output_file, "w") as f:
        f.write(json_data)
    print(f"\n✨ Generated {len(pkg_whls)} packages → {output_file}")

    # Write zstd compressed version
    zst_file = output_dir / "repodata.json.zst"
    with open(zst_file, "wb") as f:
        f.write(zstd_compress(json_bytes, level=19))
    print(f"✨ Compressed (zstd) → {zst_file}")

    return repodata_output


def generate_channeldata(repo_root: Path) -> None:
    """
    Generate channeldata.json for the channel.

    Args:
        repo_root: Root directory of the repository
    """
    channeldata = {
        "channeldata_version": 1,
        "subdirs": ["noarch"],
        "packages": {},
    }

    output_file = repo_root / "channeldata.json"
    with open(output_file, "w") as f:
        json.dump(channeldata, f, indent=2)

    print(f"✨ Generated channeldata → {output_file}")


def generate_index_html(output_dir: Path) -> None:
    """
    Generate a simple index.html for directory listing with file sizes.

    Args:
        output_dir: Directory to write index.html
    """

    def format_size(size_bytes: int) -> str:
        """Format bytes to human-readable size."""
        for unit in ["B", "KB", "MB", "GB"]:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} TB"

    # Get file sizes
    files_info = []
    for filename in ["repodata.json", "repodata.json.zst"]:
        filepath = output_dir / filename
        if filepath.exists():
            size = filepath.stat().st_size
            files_info.append((filename, format_size(size)))

    # Build file list HTML
    file_list = "\n".join(
        f'        <li><a href="{name}">{name}</a> <span class="size">({size})</span></li>'
        for name, size in files_info
    )

    html_content = f"""<!DOCTYPE html>
<html>
<head>
    <title>conda-pypi-test - noarch</title>
    <style>
        body {{ font-family: system-ui, -apple-system, sans-serif; padding: 2rem; }}
        h1 {{ color: #333; }}
        ul {{ list-style: none; padding: 0; }}
        li {{ padding: 0.5rem; }}
        a {{ color: #0066cc; text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
        .size {{ color: #666; margin-left: 1rem; font-size: 0.9em; }}
    </style>
</head>
<body>
    <h1>conda-pypi-test - noarch</h1>
    <ul>
{file_list}
    </ul>
</body>
</html>
"""

    output_file = output_dir / "index.html"
    with open(output_file, "w") as f:
        f.write(html_content)

    print(f"✨ Generated index → {output_file}")


async def main_async(concurrency: int = 100, packages_file: Path | None = None):
    """Main entry point for the async script."""
    repo_root = Path(__file__).parent
    if packages_file is None:
        packages_file = repo_root / "packages.txt"
    output_dir = repo_root / "noarch"

    if not packages_file.exists():
        print(f"❌ Error: packages.txt not found!")
        return 1

    print(f"📋 Reading packages.txt...")
    packages = parse_packages_file(packages_file)

    if not packages:
        print("⚠️  No valid packages found")
        return 1

    print(f"📍 Output directory: {output_dir}")
    print(f"🚀 Concurrency: {concurrency}\n")

    await generate_repodata(packages, output_dir, concurrency)
    generate_channeldata(repo_root)
    generate_index_html(output_dir)

    print("\n✅ Done! Run: python -m http.server 8000\n")
    return 0


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Generate conda repodata from PyPI packages with async HTTP/2"
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=100,
        help="Number of concurrent requests (default: 100)",
    )
    parser.add_argument(
        "--packages-file",
        type=Path,
        default=None,
        metavar="FILE",
        help="Path to packages file (default: packages.txt in repo root)",
    )
    args = parser.parse_args()

    return asyncio.run(main_async(args.concurrency, args.packages_file))


if __name__ == "__main__":
    exit(main())
