#!/usr/bin/env python3
"""
Tests for generate.py
"""

import json
from pathlib import Path
from unittest.mock import patch

from conda_pypi.markers import pypi_to_repodata_noarch_whl_entry
from conda_pypi.name_mapping import pypi_to_conda_name as map_package_name
from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet

from generate import parse_packages_file


def _minimal_noarch_wheel_urls(name: str, version: str) -> list[dict]:
    return [
        {
            "packagetype": "bdist_wheel",
            "filename": f"{name.replace('-', '_')}-{version}-py3-none-any.whl",
            "url": "https://example.invalid/wheel.whl",
            "size": 1,
            "digests": {"sha256": "0" * 64},
        }
    ]


def test_map_package_name_preserves_conda_underscores():
    """Grayskull conda_name underscores must be preserved so the solver can
    unify PyPI-backed packages with existing conda packages from defaults.

    """
    mapping = {
        "huggingface-hub": {"conda_name": "huggingface_hub"},
        "scikit-learn": {"conda_name": "scikit-learn"},
        "Pillow": {"conda_name": "pillow"},
    }
    with patch("conda_pypi.name_mapping.default_pypi_mapping", mapping):
        assert map_package_name("huggingface_hub") == "huggingface_hub"
        assert map_package_name("huggingface-hub") == "huggingface_hub"
        assert map_package_name("scikit-learn") == "scikit-learn"
        assert map_package_name("Pillow") == "pillow"


def test_map_package_name_falls_back_to_normalized():
    """Packages not in the grayskull mapping fall back to lowercased hyphenated name."""
    with patch("conda_pypi.name_mapping.default_pypi_mapping", {}):
        assert map_package_name("My_Package") == "my-package"
        assert map_package_name("some_lib") == "some-lib"


def test_pypi_to_repodata_whl_entry():
    """Test conversion of PyPI data to repodata entry."""
    pypi_data = {
        "info": {
            "name": "requests",
            "version": "2.32.5",
            "requires_dist": [
                "charset-normalizer <4,>=2",
                "idna <4,>=2.5",
                "urllib3 <3,>=1.21.1",
                "certifi >=2017.4.17",
            ],
            "requires_python": ">=3.8",
        },
        "urls": [
            {
                "packagetype": "bdist_wheel",
                "filename": "requests-2.32.5-py3-none-any.whl",
                "url": "https://files.pythonhosted.org/packages/.../requests-2.32.5-py3-none-any.whl",
                "size": 64928,
                "digests": {"sha256": "abc123"},
            }
        ],
    }

    entry = pypi_to_repodata_noarch_whl_entry(pypi_data)

    assert entry is not None
    assert "fn" in entry
    assert entry["name"] == "requests"
    assert entry["version"] == "2.32.5"
    assert entry["size"] == 64928
    assert entry["sha256"] == "abc123"
    assert entry["subdir"] == "noarch"
    assert entry["noarch"] == "python"
    assert "python >=3.8" in entry["depends"]
    assert len(entry["depends"]) == 5  # 4 deps + python requirement


def test_pypi_to_repodata_whl_entry_with_extras():
    """Test that extras markers go to extra_depends (conda-pyi / repodata v3)."""
    pypi_data = {
        "info": {
            "name": "httpx",
            "version": "0.28.1",
            "requires_dist": [
                "certifi",
                "httpcore ==1.*",
                "anyio; extra == 'asyncio'",
                "h2 >=3,<5; extra == 'http2'",
                "brotli; extra == 'brotli'",
                # dep with both an extra marker and a version specifier
                "socksio ==1.*; extra == 'socks'",
            ],
            "requires_python": ">=3.8",
        },
        "urls": [
            {
                "packagetype": "bdist_wheel",
                "filename": "httpx-0.28.1-py3-none-any.whl",
                "url": "https://files.pythonhosted.org/packages/.../httpx-0.28.1-py3-none-any.whl",
                "size": 109051,
                "digests": {"sha256": "def456"},
            }
        ],
    }

    entry = pypi_to_repodata_noarch_whl_entry(pypi_data)

    assert entry is not None
    assert "extra_depends" in entry

    extra_depends = entry["extra_depends"]
    assert "asyncio" in extra_depends
    assert "http2" in extra_depends
    assert "brotli" in extra_depends
    assert "socks" in extra_depends

    assert any("anyio" in dep for dep in extra_depends["asyncio"])
    assert any("h2" in dep for dep in extra_depends["http2"])
    assert any("brotli" in dep for dep in extra_depends["brotli"])
    assert any("socksio" in dep for dep in extra_depends["socks"])

    # Non-extra deps must stay in depends, not bleed into extras
    assert any("certifi" in dep for dep in entry["depends"])
    assert any("httpcore" in dep for dep in entry["depends"])
    # Extra deps must not appear in depends
    assert not any("anyio" in dep for dep in entry["depends"])
    assert not any("h2" in dep for dep in entry["depends"])


def test_pypi_to_repodata_python_marker_becomes_when_on_depends():
    """Non-extra python_version markers become [when=...] on depends (conda-pyi markers)."""
    pypi_data = {
        "info": {
            "name": "annotated-types",
            "version": "0.7.0",
            "requires_dist": ["typing-extensions>=4.0.0; python_version<'3.9'"],
            "requires_python": ">=3.8",
        },
        "urls": _minimal_noarch_wheel_urls("annotated-types", "0.7.0"),
    }
    entry = pypi_to_repodata_noarch_whl_entry(pypi_data)
    assert entry is not None
    assert any(
        dep.startswith("typing_extensions>=4.0.0") and '[when="python<3.9"]' in dep
        for dep in entry["depends"]
    ), entry["depends"]


def test_pypi_to_repodata_sys_platform_marker_becomes_virtual_when():
    """sys_platform markers map to __win / __unix style [when=...] on depends."""
    pypi_data = {
        "info": {
            "name": "platdemo",
            "version": "1.0.0",
            "requires_dist": ['colorama; sys_platform == "win32"'],
            "requires_python": ">=3.8",
        },
        "urls": _minimal_noarch_wheel_urls("platdemo", "1.0.0"),
    }
    entry = pypi_to_repodata_noarch_whl_entry(pypi_data)
    assert entry is not None
    colorama_dep = next(d for d in entry["depends"] if d.startswith("colorama"))
    assert '[when="__win"]' in colorama_dep
    assert "sys_platform" not in colorama_dep


def test_pypi_to_repodata_extra_depends_socks_maps_conda_name_and_spec():
    """PEP 508 extras land in extra_depends; PyPI names map via grayskull (cf. requests socks)."""
    pypi_data = {
        "info": {
            "name": "requests",
            "version": "2.32.5",
            "requires_dist": ["PySocks!=1.5.7,>=1.5.6; extra == 'socks'"],
            "requires_python": ">=3.8",
        },
        "urls": _minimal_noarch_wheel_urls("requests", "2.32.5"),
    }
    entry = pypi_to_repodata_noarch_whl_entry(pypi_data)
    assert entry is not None
    assert "socks" in entry["extra_depends"]
    socks_dep = entry["extra_depends"]["socks"][0]
    base_req = Requirement(socks_dep)
    assert base_req.name == "pysocks"
    assert base_req.specifier == SpecifierSet(">=1.5.6,!=1.5.7")


def test_pypi_to_repodata_whl_entry_no_extras():
    """Test that a package with no extras produces an empty extra_depends dict."""
    pypi_data = {
        "info": {
            "name": "certifi",
            "version": "2024.12.14",
            "requires_dist": None,
            "requires_python": ">=3.6",
        },
        "urls": [
            {
                "packagetype": "bdist_wheel",
                "filename": "certifi-2024.12.14-py3-none-any.whl",
                "url": "https://files.pythonhosted.org/packages/.../certifi-2024.12.14-py3-none-any.whl",
                "size": 164934,
                "digests": {"sha256": "abc000"},
            }
        ],
    }

    entry = pypi_to_repodata_noarch_whl_entry(pypi_data)

    assert entry is not None
    assert "extra_depends" in entry
    assert entry["extra_depends"] == {}


def test_pypi_to_repodata_whl_entry_no_wheel():
    """Test that None is returned when no wheel is available."""
    pypi_data = {
        "info": {
            "name": "test-package",
            "version": "1.0.0",
        },
        "urls": [
            {
                "packagetype": "sdist",
                "filename": "test-package-1.0.0.tar.gz",
            }
        ],
    }

    entry = pypi_to_repodata_noarch_whl_entry(pypi_data)
    assert entry is None


def test_parse_packages_file(tmp_path):
    """Test parsing of packages.txt file."""
    packages_file = tmp_path / "packages.txt"
    packages_file.write_text("""# Comment line
requests==2.32.5
fastapi==0.116.1

# Another comment
pydantic==2.10.5
""")

    packages = parse_packages_file(packages_file)

    assert len(packages) == 3
    assert ("requests", "2.32.5") in packages
    assert ("fastapi", "0.116.1") in packages
    assert ("pydantic", "2.10.5") in packages


def test_parse_packages_file_invalid_format(tmp_path, capsys):
    """Test parsing with invalid package format."""
    packages_file = tmp_path / "packages.txt"
    packages_file.write_text("""requests==2.32.5
invalid-package-no-version
fastapi==0.116.1
""")

    packages = parse_packages_file(packages_file)

    # Should skip invalid line but parse valid ones
    assert len(packages) == 2
    assert ("requests", "2.32.5") in packages
    assert ("fastapi", "0.116.1") in packages

    # Check warning was printed
    captured = capsys.readouterr()
    assert "Missing version" in captured.out


def test_generated_files_exist():
    """Test that all expected files are generated."""
    repo_root = Path(__file__).parent

    expected_files = [
        repo_root / "noarch" / "repodata.json",
        repo_root / "noarch" / "repodata.json.zst",
        repo_root / "noarch" / "index.html",
        repo_root / "channeldata.json",
    ]

    for filepath in expected_files:
        assert filepath.exists(), f"Expected file not found: {filepath}"


def test_repodata_structure():
    """Test that repodata.json has correct structure."""
    repo_root = Path(__file__).parent
    repodata_file = repo_root / "noarch" / "repodata.json"

    with open(repodata_file) as f:
        repodata = json.load(f)

    required_keys = [
        "info",
        "packages",
        "packages.conda",
        "v3",
        "repodata_version",
    ]
    for key in required_keys:
        assert key in repodata, f"Missing required key: {key}"

    assert isinstance(repodata["v3"], dict)
    assert "whl" in repodata["v3"]
    assert isinstance(repodata["v3"]["whl"], dict)
    assert repodata["info"]["subdir"] == "noarch"

    assert len(repodata["v3"]["whl"]) > 0, "No packages found in repodata"


def test_repodata_package_entries():
    """Test that package entries have required fields."""
    repo_root = Path(__file__).parent
    repodata_file = repo_root / "noarch" / "repodata.json"

    with open(repodata_file) as f:
        repodata = json.load(f)

    required_fields = [
        "url",
        "name",
        "version",
        "build",
        "build_number",
        "depends",
        "extra_depends",
        "fn",
        "sha256",
        "size",
        "subdir",
        "noarch",
    ]

    packages = repodata["v3"]["whl"]
    assert len(packages) > 0, "No packages to test"

    first_package = next(iter(packages.values()))
    for field in required_fields:
        assert field in first_package, f"Missing required field: {field}"

    # Validate field types
    assert isinstance(first_package["name"], str)
    assert isinstance(first_package["version"], str)
    assert isinstance(first_package["depends"], list)
    assert isinstance(first_package["size"], int)
    assert first_package["subdir"] == "noarch"
    assert first_package["noarch"] == "python"


def test_channeldata_structure():
    """Test that channeldata.json has correct structure."""
    repo_root = Path(__file__).parent
    channeldata_file = repo_root / "channeldata.json"

    with open(channeldata_file) as f:
        channeldata = json.load(f)

    assert "channeldata_version" in channeldata
    assert channeldata["channeldata_version"] == 1
    assert "subdirs" in channeldata
    assert "noarch" in channeldata["subdirs"]


def test_repodata_extra_depends_field_present():
    """Test that every package entry has extra_depends (repodata v3 whl shape)."""
    repo_root = Path(__file__).parent
    repodata_file = repo_root / "noarch" / "repodata.json"

    with open(repodata_file) as f:
        repodata = json.load(f)

    packages = repodata["v3"]["whl"]
    assert len(packages) > 0, "No packages to test"

    for key, entry in packages.items():
        assert "extra_depends" in entry, f"Package {key} is missing 'extra_depends' field"
        assert isinstance(entry["extra_depends"], dict), (
            f"Package {key} 'extra_depends' is not a dict"
        )


def test_repodata_extra_depends_not_in_depends():
    """Test that extra_depends are not duplicated in the top-level depends list."""
    repo_root = Path(__file__).parent
    repodata_file = repo_root / "noarch" / "repodata.json"

    with open(repodata_file) as f:
        repodata = json.load(f)

    packages = repodata["v3"]["whl"]
    for key, entry in packages.items():
        extra_deps = {dep for deps in entry.get("extra_depends", {}).values() for dep in deps}
        for dep in entry.get("depends", []):
            assert dep not in extra_deps, (
                f"Package {key}: dep '{dep}' appears in both depends and extra_depends"
            )


def test_repodata_has_packages_with_extra_depends():
    """At least one package should have non-empty extra_depends (fixture includes httpx, etc.)."""
    repo_root = Path(__file__).parent
    repodata_file = repo_root / "noarch" / "repodata.json"

    with open(repodata_file) as f:
        repodata = json.load(f)

    packages = repodata["v3"]["whl"]
    with_extra = [k for k, v in packages.items() if v.get("extra_depends")]
    assert len(with_extra) > 0, (
        "No packages with extra_depends found in repodata. "
        "Ensure packages-test.txt includes packages that declare extras "
        "(e.g. httpx, requests)."
    )


def test_compressed_files_valid():
    """Test that compressed files are valid and non-empty."""
    repo_root = Path(__file__).parent

    # Check zst file
    zst_file = repo_root / "noarch" / "repodata.json.zst"
    assert zst_file.exists()
    assert zst_file.stat().st_size > 0

    # Compressed file should be smaller than uncompressed
    json_file = repo_root / "noarch" / "repodata.json"
    json_size = json_file.stat().st_size
    zst_size = zst_file.stat().st_size

    assert zst_size < json_size, "zst file should be smaller than json"
