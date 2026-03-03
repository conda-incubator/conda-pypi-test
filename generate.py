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
import re
import time
from packaging.requirements import Requirement, InvalidRequirement
from pathlib import Path
from typing import Any


# Grayskull mapping URL
GRAYSKULL_MAPPING_URL = "https://raw.githubusercontent.com/regro/cf-graph-countyfair/master/mappings/pypi/grayskull_pypi_mapping.json"

# Global cache for the mapping data
_MAPPING_CACHE: dict[str, dict[str, str]] | None = None


async def load_grayskull_mapping() -> dict[str, dict[str, str]]:
    """Load grayskull PyPI to conda mapping from conda-pypi repository."""
    global _MAPPING_CACHE

    if _MAPPING_CACHE is not None:
        return _MAPPING_CACHE

    try:
        async with httpx.AsyncClient(http2=True, timeout=30.0) as client:
            response = await client.get(
                GRAYSKULL_MAPPING_URL,
                headers={"Accept-Encoding": "gzip, deflate, br"},
            )
            response.raise_for_status()
            _MAPPING_CACHE = response.json()
    except Exception:
        print(f"⚠️  Warning: Could not load grayskull mapping")
        print(f"   Using fallback name normalization only")
        _MAPPING_CACHE = {}

    return _MAPPING_CACHE


def normalize_name(name: str) -> str:
    """
    Normalize a package name to conda conventions.
    Converts to lowercase and replaces underscores with hyphens.
    """
    return name.lower().replace("_", "-")


def map_package_name(pypi_name: str) -> str:
    """
    Map a PyPI package name to its conda equivalent using grayskull mapping.
    Falls back to normalized name if no mapping exists.
    """
    mapping = _MAPPING_CACHE or {}
    normalized = normalize_name(pypi_name)

    if normalized in mapping:
        return normalize_name(mapping[normalized].get("conda_name", normalized))

    return normalized


def pypi_to_repodata_whl_entry(
    pypi_data: dict[str, Any], url_index: int = 0
) -> dict[str, Any] | None:
    """
    Convert PyPI JSON endpoint data to a repodata.json packages.whl entry.

    Args:
        pypi_data: Dictionary containing the complete info section from PyPI JSON endpoint
        url_index: Index of the wheel URL to use (typically the first one is the wheel)

    Returns:
        Dictionary representing the entry for packages.whl, or None if wheel not found
    """
    # Find a pure Python wheel (platform tag must be "none-any").
    # Wheels with compiled native code use platform-specific tags such as
    # "cp311-cp311-manylinux_2_17_x86_64" and must be excluded — conda
    # installs them differently via the native package manager.
    wheel_url = None

    for url_entry in pypi_data.get("urls", []):
        if url_entry.get("packagetype") != "bdist_wheel":
            continue
        filename = url_entry.get("filename", "")
        if not filename.endswith("-none-any.whl"):
            continue
        wheel_url = url_entry
        break

    if not wheel_url:
        return None

    pypi_info = pypi_data.get("info")

    # Extract and map package name
    pypi_name = pypi_info.get("name")
    conda_name = map_package_name(pypi_name)
    version = pypi_info.get("version")

    # Build dependency list and extras dict with name mapping
    depends_list = []
    extras_dict: dict[str, list[str]] = {}
    for dep in pypi_info.get("requires_dist") or []:
        try:
            req = Requirement(dep)
        except InvalidRequirement:
            # Not logging failures here because there may be too many with 500K+ packages
            continue

        conda_dep = map_package_name(req.name) + str(req.specifier)

        if req.marker:
            extra_match = re.search(
                r'extra\s*==\s*["\']([^"\']+)["\']', str(req.marker)
            )
            if extra_match:
                extras_dict.setdefault(extra_match.group(1), []).append(conda_dep)
            else:
                depends_list.append(conda_dep)
        else:
            depends_list.append(conda_dep)

    # Add Python version requirement
    python_requires = pypi_info.get("requires_python")
    if python_requires:
        depends_list.append(f"python {python_requires}")
    else:
        # Noarch python packages should still depend on python when PyPI omits requires_python
        depends_list.append("python")

    # Extract filename components
    filename = wheel_url.get("filename", "")

    # Build the repodata entry
    entry = {
        "url": wheel_url.get("url", ""),
        "record_version": 3,
        "name": conda_name,
        "version": version,
        "build": "py3_none_any_0",
        "build_number": 0,
        "depends": depends_list,
        "extras": extras_dict,
        "sha256": wheel_url.get("digests", {}).get("sha256", ""),
        "size": wheel_url.get("size", 0),
        "subdir": "noarch",
        "noarch": "python",
    }

    return entry


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

            return pypi_to_repodata_whl_entry(pypi_data)
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

            # Parse package specification (format: package-name==version)
            if "==" in line:
                parts = line.split("==", 1)
                if len(parts) == 2:
                    name, version = parts
                    packages.append((name.strip(), version.strip()))
                else:
                    print(f"  ⚠️  Invalid format on line {line_num}: {line}")
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

    Raises:
        SystemExit: If any packages are missing wheels
    """
    pkg_whls = {}
    failed_packages = []

    print(f"📦 Fetching {len(packages)} packages with async HTTP/2...\n")

    # Create async client with HTTP/2
    try:
        client = httpx.AsyncClient(
            http2=True,
            limits=httpx.Limits(
                max_connections=concurrency,
                max_keepalive_connections=50,
                keepalive_expiry=60.0,
            ),
            timeout=httpx.Timeout(30.0, connect=10.0),
        )
    except Exception:
        # Fallback to HTTP/1.1
        client = httpx.AsyncClient(
            http2=False,
            limits=httpx.Limits(
                max_connections=concurrency * 2,
                max_keepalive_connections=100,
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
                    conda_name = entry["name"]
                    key = f"{conda_name}-{version}-py3_none_any_0"
                    pkg_whls[key] = entry

                    # Show progress with rate every 100 packages or on completion
                    if completed % 100 == 0 or completed == len(packages):
                        print(
                            f"  ✅ [{completed}/{len(packages)}] {name} {version} ({rate:.1f}/s)"
                        )
                    # Show mapping if name was changed (only occasionally)
                    elif conda_name != normalize_name(name):
                        print(
                            f"  ✅ [{completed}/{len(packages)}] {name} {version} → {conda_name}"
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

    # Build repodata structure
    repodata_output = {
        "info": {"subdir": "noarch"},
        "packages": {},
        "packages.conda": {},
        "removed": [],
        "repodata_version": 1,
        "signatures": {},
        "packages.whl": {key: value for key, value in sorted(pkg_whls.items())},
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

    # Load mapping early to catch errors before processing
    await load_grayskull_mapping()

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
