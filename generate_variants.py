#!/usr/bin/env python3
"""
Generate variant-aware conda repodata from PEP 825 variant wheel registries.

Fetches -variants.json from wheelnext registries, discovers available variant
wheels, and produces repodata with virtual package deps (__cuda, __cuda_arch)
and runtime deps (cuda-toolkit) so conda's solver selects the right variant.

Usage:
    python generate_variants.py [--concurrency N]
"""

import argparse
import asyncio
import json
import logging
import re
import time
from pathlib import Path
from typing import Any

import httpx

log = logging.getLogger(__name__)

# Variant registries: package name -> (registry_url, simple_page_url)
# From wheelnext/variants-index/index.toml
VARIANT_REGISTRIES: dict[str, str] = {
    # NVIDIA CUDA libs
    "nvidia-cublas": "https://variants-index.wheelnext.dev/",
    "nvidia-cuda-cccl": "https://variants-index.wheelnext.dev/",
    "nvidia-cuda-cupti": "https://variants-index.wheelnext.dev/",
    "nvidia-cuda-nvrtc": "https://variants-index.wheelnext.dev/",
    "nvidia-cuda-runtime": "https://variants-index.wheelnext.dev/",
    "nvidia-cudnn": "https://variants-index.wheelnext.dev/",
    "nvidia-cufft": "https://variants-index.wheelnext.dev/",
    "nvidia-curand": "https://variants-index.wheelnext.dev/",
    "nvidia-cusolver": "https://variants-index.wheelnext.dev/",
    "nvidia-cusparse": "https://variants-index.wheelnext.dev/",
    "nvidia-cusparselt": "https://variants-index.wheelnext.dev/",
    "nvidia-nccl": "https://variants-index.wheelnext.dev/",
    "nvidia-nvjitlink": "https://variants-index.wheelnext.dev/",
    "nvidia-nvtx": "https://variants-index.wheelnext.dev/",
    # PyTorch
    "torch": "https://download.pytorch.org/whl/variant/",
    "torchvision": "https://download.pytorch.org/whl/variant/",
    # Triton
    "triton": "https://download.pytorch.org/whl/variant/",
    "triton-rocm": "https://download.pytorch.org/whl/variant/",
    "triton-xpu": "https://download.pytorch.org/whl/variant/",
    # Other
    "cupy": "https://variants-index.wheelnext.dev/",
    "xgboost": "https://wheels-variant.xgboost-ci.com/",
    "transformer-engine": "https://variants-index.wheelnext.dev/",
}

# Wheel platform tag -> conda subdir
PLATFORM_TO_SUBDIR = {
    ("manylinux", "x86_64"): "linux-64",
    ("manylinux", "aarch64"): "linux-aarch64",
    ("linux", "x86_64"): "linux-64",
    ("linux", "aarch64"): "linux-aarch64",
    ("macosx", "x86_64"): "osx-64",
    ("macosx", "arm64"): "osx-arm64",
    ("win", "amd64"): "win-64",
    ("win", "arm64"): "win-arm64",
}

# Variant property -> conda deps mapping
# Returns (virtual_deps, runtime_deps) for a given namespace::feature::value
VARIANT_PROPERTY_MAP = {
    ("nvidia", "cuda_version_lower_bound"): lambda v: (
        [f"__cuda >={v}"],
        [f"cuda-toolkit >={v}"],
    ),
    ("nvidia", "cuda_version_upper_bound"): lambda v: (
        [f"__cuda <{v}"],
        [f"cuda-toolkit <{v}"],
    ),
    ("nvidia", "sm_arch"): lambda v: (
        # sm_arch like "90_real" -> __cuda_arch >=9.0
        # NOTE: __cuda_arch is provided by nvidia-virtual-packages plugin (conda-incubator),
        # not part of CEP-30 core virtual packages. CEP pending: conda/ceps PR #157.
        # Without the plugin installed, this dep is unsatisfiable and the solver skips the variant.
        [f"__cuda_arch >={_sm_to_version(v)}"] if "_real" in v else [],
        [],
    ),
}


def _sm_to_version(sm: str) -> str:
    """Convert SM arch string like '90_real' to version '9.0'."""
    digits = re.match(r"(\d+)", sm)
    if not digits:
        return "0.0"
    num = digits.group(1)
    if len(num) >= 2:
        return f"{num[:-1]}.{num[-1]}"
    return f"{num}.0"


def _python_tag_to_dep(tag: str) -> str | None:
    """Convert wheel python tag to conda python dependency.

    cp310 -> 'python >=3.10'
    cp313 -> 'python >=3.13'
    py3   -> 'python >=3'
    py2.py3 -> 'python'
    """
    if tag.startswith("cp"):
        digits = tag[2:]
        if len(digits) >= 2:
            return f"python >={digits[0]}.{digits[1:]}"
        return f"python >={digits}"
    if tag == "py3":
        return "python >=3"
    if tag == "py2.py3":
        return "python"
    return "python"


def variant_to_conda_deps(variant_props: dict[str, dict[str, list[str]]]) -> list[str]:
    """Convert variant properties to conda dependency strings.

    Args:
        variant_props: e.g. {"nvidia": {"cuda_version_lower_bound": ["12.0"], "sm_arch": ["90_real"]}}

    Returns:
        List of conda dep strings like ["__cuda >=12.0", "cuda-toolkit >=12.0", "__cuda_arch >=9.0"]
    """
    deps = []
    for namespace, features in variant_props.items():
        for feature, values in features.items():
            mapper = VARIANT_PROPERTY_MAP.get((namespace, feature))
            if mapper:
                # Use the first value (most relevant)
                for value in values[:1]:
                    virtual_deps, runtime_deps = mapper(value)
                    deps.extend(virtual_deps)
                    deps.extend(runtime_deps)
    return deps


def wheel_filename_to_subdir(filename: str) -> str | None:
    """Parse wheel filename platform tag to conda subdir."""
    # Remove variant label if present (last field after platform)
    # torch-2.10.0-cp310-cp310-manylinux_2_28_x86_64-cuda12.6.whl
    parts = filename.removesuffix(".whl").split("-")
    # Standard wheel: name-ver-py-abi-plat.whl (5 parts)
    # Variant wheel: name-ver-py-abi-plat-label.whl (6 parts)
    if len(parts) < 5:
        return None

    # Platform tag is always the 5th part (index 4)
    platform_tag = parts[4]

    for (prefix, arch), subdir in PLATFORM_TO_SUBDIR.items():
        if platform_tag.startswith(prefix) and platform_tag.endswith(arch):
            return subdir
    return None


def glibc_from_platform_tag(filename: str) -> str | None:
    """Extract glibc requirement from manylinux tag."""
    parts = filename.removesuffix(".whl").split("-")
    if len(parts) < 5:
        return None
    platform_tag = parts[4]
    match = re.match(r"manylinux_(\d+)_(\d+)", platform_tag)
    if match:
        return f"{match.group(1)}.{match.group(2)}"
    return None


async def fetch_simple_page(
    package_name: str, registry_url: str, client: httpx.AsyncClient
) -> str | None:
    """Fetch a PEP 503 simple page for a package."""
    # Normalize package name for URL (PEP 503)
    normalized = re.sub(r"[-_.]+", "-", package_name).lower()
    url = f"{registry_url}{normalized}/"
    try:
        response = await client.get(url)
        response.raise_for_status()
        return response.text
    except Exception as e:
        log.debug(f"Failed to fetch simple page for {package_name}: {e}")
        return None


async def fetch_variants_json(
    package_name: str, version: str, registry_url: str, client: httpx.AsyncClient
) -> dict[str, Any] | None:
    """Fetch {name}-{version}-variants.json from a registry."""
    normalized = re.sub(r"[-_.]+", "_", package_name).lower()
    # Try to find the variants.json URL from the simple page
    page = await fetch_simple_page(package_name, registry_url, client)
    if not page:
        return None

    # Look for the variants.json link
    pattern = rf'href="([^"]*{re.escape(normalized)}-{re.escape(version)}-variants\.json[^"]*)"'
    match = re.search(pattern, page)
    if not match:
        return None

    json_path = match.group(1)
    # Resolve relative/absolute URL
    if json_path.startswith("http"):
        json_url = json_path
    elif json_path.startswith("/"):
        # Absolute path - extract base from registry_url
        from urllib.parse import urlparse

        parsed = urlparse(registry_url)
        json_url = f"{parsed.scheme}://{parsed.netloc}{json_path}"
    else:
        json_url = f"{registry_url}{re.sub(r'[-_.]+', '-', package_name).lower()}/{json_path}"

    try:
        response = await client.get(json_url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        log.debug(f"Failed to fetch variants.json for {package_name} {version}: {e}")
        return None


def parse_wheel_links(page_html: str, version: str) -> list[dict[str, str]]:
    """Extract wheel URLs, filenames, and sha256 hashes from a simple page."""
    wheels = []
    for match in re.finditer(r'href="([^"]*)"', page_html):
        href = match.group(1)
        # Extract sha256 from fragment (href="...whl#sha256=abc123")
        sha256 = None
        if "#sha256=" in href:
            sha256 = href.split("#sha256=")[1]
            url = href.split("#")[0]
        else:
            url = href.split("#")[0]
        if not url.endswith(".whl"):
            continue
        filename = url.rsplit("/", 1)[-1]
        if f"-{version}-" not in filename:
            continue
        wheels.append({"url": url, "filename": filename, "sha256": sha256})
    return wheels


def build_variant_repodata_entry(
    wheel: dict[str, str],
    variant_label: str,
    variant_props: dict[str, dict[str, list[str]]],
    build_number: int,
    registry_url: str,
) -> tuple[str, dict[str, Any]] | None:
    """Build a single repodata entry for a variant wheel."""
    filename = wheel["filename"]
    url = wheel["url"]

    # Resolve URL
    if url.startswith("/"):
        from urllib.parse import urlparse

        parsed = urlparse(registry_url)
        url = f"{parsed.scheme}://{parsed.netloc}{url}"
    elif not url.startswith("http"):
        url = f"{registry_url}{url}"

    subdir = wheel_filename_to_subdir(filename)
    if not subdir:
        return None

    # Parse name and version from filename
    parts = filename.removesuffix(".whl").split("-")
    if len(parts) < 5:
        return None

    name = parts[0].replace("_", "-").lower()
    version = parts[1]
    python_tag = parts[2]
    abi_tag = parts[3]

    # Build conda deps from variant properties
    variant_deps = variant_to_conda_deps(variant_props)

    # Add glibc dep from platform tag
    glibc_ver = glibc_from_platform_tag(filename)
    if glibc_ver:
        variant_deps.append(f"__glibc >={glibc_ver}")

    # Base deps - parse python version from tag
    python_dep = _python_tag_to_dep(python_tag)
    depends = [python_dep] if python_dep else []
    depends.extend(variant_deps)

    # Build string
    platform_short = subdir.replace("-", "_")
    if variant_label and variant_label != "null":
        build = f"{python_tag}_{platform_short}_{variant_label}_{build_number}"
    else:
        build = f"{python_tag}_{platform_short}_{build_number}"

    # Repodata key
    key = f"{name}-{version}-{build}"

    entry = {
        "name": name,
        "version": version,
        "build": build,
        "build_number": build_number,
        "depends": depends,
        "url": url,
        "fn": filename,
        "subdir": subdir,
        "md5": None,
        "sha256": wheel.get("sha256"),
        "size": None,
        "timestamp": 0,
    }

    return key, entry


async def process_variant_package(
    name: str,
    version: str,
    registry_url: str,
    client: httpx.AsyncClient,
) -> list[tuple[str, dict[str, Any], str]]:
    """Process a single package: fetch variants.json, build repodata entries.

    Returns list of (repodata_key, entry_dict, subdir) tuples.
    """
    variants_json = await fetch_variants_json(name, version, registry_url, client)
    if not variants_json:
        log.info(f"No variants.json found for {name}=={version}")
        return []

    variants = variants_json.get("variants", {})
    if not variants:
        return []

    # Fetch simple page to get wheel URLs
    page = await fetch_simple_page(name, registry_url, client)
    if not page:
        return []

    wheels = parse_wheel_links(page, version)
    if not wheels:
        log.info(f"No wheels found for {name}=={version}")
        return []

    # Assign build numbers by priority (null=0, others ascending)
    # Higher priority variants get higher build numbers
    priority_order = variants_json.get("default-priorities", {}).get("namespace", [])
    variant_labels = list(variants.keys())

    # Sort: null last (lowest build_number), others by priority field if present
    def variant_sort_key(label):
        if label == "null":
            return (1, "")  # null sorts last
        props = variants.get(label, {})
        # Use static "priority::order" if available
        priority_val = props.get("priority", {}).get("order", ["99"])
        return (0, priority_val[0] if priority_val else "99")

    sorted_labels = sorted(variant_labels, key=variant_sort_key)
    # Reverse so highest priority gets highest build_number
    sorted_labels.reverse()

    entries = []
    for build_num, label in enumerate(sorted_labels):
        variant_props = variants[label]

        # Find matching wheel(s) for this variant label
        for wheel in wheels:
            fn = wheel["filename"]
            # Variant wheels end with -{label}.whl
            if label == "null":
                # Null variant: filename ends with -{platform}-null.whl
                if not fn.endswith(f"-null.whl"):
                    continue
            else:
                if not fn.endswith(f"-{label}.whl"):
                    continue

            result = build_variant_repodata_entry(
                wheel, label, variant_props, build_num, registry_url
            )
            if result:
                key, entry = result
                subdir = entry["subdir"]
                entries.append((key, entry, subdir))

    return entries


def parse_variant_packages_file(filepath: Path) -> list[tuple[str, str]]:
    """Parse variant-packages.txt: name==version lines."""
    packages = []
    with open(filepath) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "==" in line:
                name, version = line.split("==", 1)
                packages.append((name.strip(), version.strip()))
    return packages


async def generate_variant_repodata(
    packages: list[tuple[str, str]], repo_root: Path, concurrency: int = 20
) -> None:
    """Generate variant-aware repodata for the given packages."""
    # Collect entries grouped by subdir
    entries_by_subdir: dict[str, dict[str, Any]] = {}

    print(f"Fetching variant metadata for {len(packages)} packages...\n")

    client = httpx.AsyncClient(
        http2=True,
        limits=httpx.Limits(max_connections=concurrency, max_keepalive_connections=10),
        timeout=httpx.Timeout(30.0, connect=10.0),
        follow_redirects=True,
    )

    try:
        semaphore = asyncio.Semaphore(concurrency)

        async def fetch_one(name: str, version: str):
            async with semaphore:
                registry_url = VARIANT_REGISTRIES.get(name)
                if not registry_url:
                    print(f"  skip {name}=={version} (no registry configured)")
                    return []
                return await process_variant_package(name, version, registry_url, client)

        tasks = [fetch_one(name, version) for name, version in packages]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        total_entries = 0
        for i, result in enumerate(results):
            name, version = packages[i]
            if isinstance(result, Exception):
                print(f"  error {name}=={version}: {result}")
                continue
            for key, entry, subdir in result:
                entries_by_subdir.setdefault(subdir, {})[key] = entry
                total_entries += 1
            if result:
                print(f"  {name}=={version}: {len(result)} variant entries")

    finally:
        await client.aclose()

    if not entries_by_subdir:
        print("\nNo variant entries produced.")
        return

    # Write repodata per subdir
    print(f"\nWriting repodata for {len(entries_by_subdir)} subdirs ({total_entries} total entries)...")

    for subdir, whl_entries in entries_by_subdir.items():
        subdir_dir = repo_root / subdir
        subdir_dir.mkdir(parents=True, exist_ok=True)

        repodata = {
            "info": {"subdir": subdir},
            "packages": {},
            "packages.conda": {},
            "removed": [],
            "repodata_version": 1,
            "v3": {
                "conda": {},
                "tar.bz2": {},
                "whl": whl_entries,
            },
        }

        json_data = json.dumps(repodata, indent=2)
        (subdir_dir / "repodata.json").write_text(json_data)

        # Compressed version
        import zstandard as zstd

        cctx = zstd.ZstdCompressor(level=19)
        (subdir_dir / "repodata.json.zst").write_bytes(
            cctx.compress(json_data.encode())
        )

        print(f"  {subdir}/repodata.json: {len(whl_entries)} entries")

    # Update channeldata
    channeldata = {
        "channeldata_version": 1,
        "subdirs": sorted(entries_by_subdir.keys()),
        "packages": {},
    }
    (repo_root / "channeldata.json").write_text(json.dumps(channeldata, indent=2))
    print(f"\nDone. Subdirs: {', '.join(sorted(entries_by_subdir.keys()))}")


async def main_async(concurrency: int = 20, packages_file: Path | None = None):
    repo_root = Path(__file__).parent
    if packages_file is None:
        packages_file = repo_root / "variant-packages.txt"

    if not packages_file.exists():
        print(f"Error: {packages_file} not found!")
        return 1

    packages = parse_variant_packages_file(packages_file)
    if not packages:
        print("No packages found")
        return 1

    await generate_variant_repodata(packages, repo_root, concurrency)
    return 0


def main():
    parser = argparse.ArgumentParser(
        description="Generate variant-aware conda repodata from PEP 825 registries"
    )
    parser.add_argument(
        "--concurrency", type=int, default=20, help="Concurrent requests (default: 20)"
    )
    parser.add_argument(
        "--packages-file", type=Path, default=None, help="Path to variant-packages.txt"
    )
    args = parser.parse_args()
    return asyncio.run(main_async(args.concurrency, args.packages_file))


if __name__ == "__main__":
    exit(main())
