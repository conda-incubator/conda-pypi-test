#!/usr/bin/env python3
"""
Tests for generate.py
"""

import json
from pathlib import Path
from generate import (
    pypi_to_repodata_whl_entry,
    parse_packages_file,
)


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
    
    entry = pypi_to_repodata_whl_entry(pypi_data)
    
    assert entry is not None
    assert entry["name"] == "requests"
    assert entry["version"] == "2.32.5"
    assert entry["fn"] == "requests-2.32.5-py3-none-any.whl"
    assert entry["size"] == 64928
    assert entry["sha256"] == "abc123"
    assert entry["subdir"] == "noarch"
    assert entry["noarch"] == "python"
    assert "python >=3.8" in entry["depends"]
    assert len(entry["depends"]) == 5  # 4 deps + python requirement


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
    
    entry = pypi_to_repodata_whl_entry(pypi_data)
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
        repo_root / "noarch" / "repodata.json.bz2",
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
    
    # Check required top-level keys
    required_keys = ["info", "packages", "packages.conda", "packages.whl", "repodata_version"]
    for key in required_keys:
        assert key in repodata, f"Missing required key: {key}"
    
    # Check structure
    assert isinstance(repodata["packages.whl"], dict)
    assert repodata["info"]["subdir"] == "noarch"
    assert repodata["repodata_version"] == 1
    
    # Check that we have at least one package
    assert len(repodata["packages.whl"]) > 0, "No packages found in repodata"


def test_repodata_package_entries():
    """Test that package entries have required fields."""
    repo_root = Path(__file__).parent
    repodata_file = repo_root / "noarch" / "repodata.json"
    
    with open(repodata_file) as f:
        repodata = json.load(f)
    
    required_fields = [
        "url",
        "record_version",
        "name",
        "version",
        "build",
        "build_number",
        "depends",
        "fn",
        "sha256",
        "size",
        "subdir",
        "noarch",
    ]
    
    # Check first package entry
    packages = repodata["packages.whl"]
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


def test_compressed_files_valid():
    """Test that compressed files are valid and non-empty."""
    repo_root = Path(__file__).parent
    
    # Check bz2 file
    bz2_file = repo_root / "noarch" / "repodata.json.bz2"
    assert bz2_file.exists()
    assert bz2_file.stat().st_size > 0
    
    # Check zst file
    zst_file = repo_root / "noarch" / "repodata.json.zst"
    assert zst_file.exists()
    assert zst_file.stat().st_size > 0
    
    # Compressed files should be smaller than uncompressed
    json_file = repo_root / "noarch" / "repodata.json"
    json_size = json_file.stat().st_size
    bz2_size = bz2_file.stat().st_size
    zst_size = zst_file.stat().st_size
    
    assert bz2_size < json_size, "bz2 file should be smaller than json"
    assert zst_size < json_size, "zst file should be smaller than json"
