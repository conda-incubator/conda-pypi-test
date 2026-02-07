#!/usr/bin/env python3
"""
Generate conda repodata.json from PyPI packages.

This script fetches package metadata from PyPI and converts it to conda-compatible
repodata format with packages.whl entries for use with conda-pypi.
"""

import json
import bz2
import zstandard as zstd
import requests
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import time


# Grayskull mapping URL
GRAYSKULL_MAPPING_URL = "https://raw.githubusercontent.com/regro/cf-graph-countyfair/master/mappings/pypi/grayskull_pypi_mapping.json"

# Global cache for the mapping data
_MAPPING_CACHE: Optional[Dict[str, Dict[str, str]]] = None


def load_grayskull_mapping() -> Dict[str, Dict[str, str]]:
    """Load grayskull PyPI to conda mapping from conda-pypi repository."""
    global _MAPPING_CACHE

    if _MAPPING_CACHE is not None:
        return _MAPPING_CACHE

    try:
        response = requests.get(GRAYSKULL_MAPPING_URL, timeout=30)
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
    mapping = load_grayskull_mapping()
    normalized = normalize_name(pypi_name)

    if normalized in mapping:
        return normalize_name(mapping[normalized].get("conda_name", normalized))

    return normalized


def map_dependency_name(pypi_dep: str) -> str:
    """
    Map a PyPI dependency string to use conda package names.
    Handles version specifiers like "package>=1.0".
    """
    match = re.match(r"^([a-zA-Z0-9._-]+)", pypi_dep)
    if not match:
        return pypi_dep

    pypi_name = match.group(1)
    conda_name = map_package_name(pypi_name)
    return pypi_dep.replace(pypi_name, conda_name, 1)


def pypi_to_repodata_whl_entry(
    pypi_data: Dict[str, Any], url_index: int = 0
) -> Optional[Dict[str, Any]]:
    """
    Convert PyPI JSON endpoint data to a repodata.json packages.whl entry.

    Args:
        pypi_data: Dictionary containing the complete info section from PyPI JSON endpoint
        url_index: Index of the wheel URL to use (typically the first one is the wheel)

    Returns:
        Dictionary representing the entry for packages.whl, or None if wheel not found
    """
    # Find the wheel URL (bdist_wheel package type)
    wheel_url = None

    for url_entry in pypi_data.get("urls", []):
        if url_entry.get("packagetype") == "bdist_wheel":
            wheel_url = url_entry
            break

    if not wheel_url:
        return None

    pypi_info = pypi_data.get("info")

    # Extract and map package name
    pypi_name = pypi_info.get("name")
    conda_name = map_package_name(pypi_name)
    version = pypi_info.get("version")

    # Build dependency list with name mapping
    depends_list = []
    for dep in pypi_info.get("requires_dist") or []:
        if "extra" not in dep:
            # Remove environment markers (after semicolon)
            dep_clean = dep.split(";")[0].strip()
            # Map the dependency name to conda equivalent
            conda_dep = map_dependency_name(dep_clean)
            depends_list.append(conda_dep)

    # Add Python version requirement
    python_requires = pypi_info.get("requires_python")
    if python_requires:
        depends_list.append(f"python {python_requires}")

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
        "sha256": wheel_url.get("digests", {}).get("sha256", ""),
        "size": wheel_url.get("size", 0),
        "subdir": "noarch",
        "noarch": "python",
    }

    return entry


def get_repodata_entry(name: str, version: str, max_retries: int = 3) -> Optional[Dict[str, Any]]:
    """
    Fetch package data from PyPI and convert to repodata entry.

    Args:
        name: Package name
        version: Package version
        max_retries: Maximum number of retry attempts

    Returns:
        Repodata entry dictionary or None if failed
    """
    pypi_endpoint = f"https://pypi.org/pypi/{name}/{version}/json"

    for attempt in range(max_retries):
        try:
            response = requests.get(pypi_endpoint, timeout=30)
            response.raise_for_status()
            pypi_data = response.json()

            if not pypi_data:
                return None

            return pypi_to_repodata_whl_entry(pypi_data)
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                print(f"  ⚠️  Error fetching {name} {version} (attempt {attempt + 1}/{max_retries}): {e}")
                print(f"     Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                print(f"  ❌ Error fetching {name} {version} after {max_retries} attempts: {e}")
                return None
        except Exception as e:
            print(f"  ❌ Error processing {name} {version}: {e}")
            return None
    
    return None


def parse_packages_file(filepath: Path) -> List[Tuple[str, str]]:
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


def generate_repodata(
    packages: List[Tuple[str, str]], output_dir: Path, max_workers: int = 25
) -> Dict[str, Any]:
    """
    Generate repodata.json from list of packages.

    Args:
        packages: List of (name, version) tuples
        output_dir: Directory to write repodata.json
        max_workers: Maximum number of parallel workers

    Returns:
        Generated repodata dictionary

    Raises:
        SystemExit: If any packages are missing wheels
    """
    pkg_whls = {}
    failed_packages = []

    print(f"📦 Fetching {len(packages)} packages...\n")

    # Run in parallel using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Map each package to its repodata entry
        futures = {
            executor.submit(get_repodata_entry, pkg_tuple[0], pkg_tuple[1]): pkg_tuple
            for pkg_tuple in packages
        }

        # Collect results as they complete
        completed = 0
        for future in as_completed(futures):
            pkg_tuple = futures[future]
            name, version = pkg_tuple
            completed += 1

            result = future.result()
            if result:
                conda_name = result["name"]
                key = f"{conda_name}-{version}-py3_none_any_0"
                pkg_whls[key] = result

                # Show mapping if name was changed
                if conda_name != normalize_name(name):
                    print(
                        f"  ✅ [{completed}/{len(packages)}] {name} {version} → {conda_name}"
                    )
                else:
                    print(f"  ✅ [{completed}/{len(packages)}] {name} {version}")
            else:
                failed_packages.append(f"{name}=={version}")
                print(
                    f"  ⚠️  [{completed}/{len(packages)}] {name} {version} - no wheel found"
                )

    # Check if any packages failed
    if failed_packages:
        print(f"\n❌ ERROR: {len(failed_packages)} package(s) missing wheels:\n")
        for pkg in sorted(failed_packages):
            print(f"   - {pkg}")
        print(f"\nPlease remove these packages from packages.txt and try again.\n")
        raise SystemExit(1)

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

    # Write bz2 compressed version
    bz2_file = output_dir / "repodata.json.bz2"
    with open(bz2_file, "wb") as f:
        f.write(bz2.compress(json_bytes))
    print(f"✨ Compressed (bz2) → {bz2_file}")

    # Write zstd compressed version
    zst_file = output_dir / "repodata.json.zst"
    cctx = zstd.ZstdCompressor(level=19)
    with open(zst_file, "wb") as f:
        f.write(cctx.compress(json_bytes))
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
    for filename in ["repodata.json", "repodata.json.bz2", "repodata.json.zst"]:
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


def main():
    """Main entry point for the script."""
    repo_root = Path(__file__).parent
    packages_file = repo_root / "packages.txt"
    output_dir = repo_root / "noarch"

    if not packages_file.exists():
        print(f"❌ Error: packages.txt not found!")
        return 1

    # Load mapping early to catch errors before processing
    load_grayskull_mapping()

    print(f"📋 Reading packages.txt...")
    packages = parse_packages_file(packages_file)

    if not packages:
        print("⚠️  No valid packages found")
        return 1

    print(f"📍 Output directory: {output_dir}\n")

    generate_repodata(packages, output_dir)
    generate_channeldata(repo_root)
    generate_index_html(output_dir)

    print("\n✅ Done! Run: python -m http.server 8000\n")
    return 0


if __name__ == "__main__":
    exit(main())
