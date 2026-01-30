# dev-channel

A simple development conda channel for testing repodata.

## Setup

Create the conda environment:
```bash
conda env create -f environment.yml
conda activate dev-channel
```

## Quick Start

1. Edit `packages.txt` with your desired packages:
```
requests@2.32.5
fastapi@0.116.1
```

2. Generate repodata:
```bash
python generate.py
```

This generates:
- `noarch/repodata.json` - Uncompressed metadata
- `noarch/repodata.json.bz2` - Bzip2 compressed (faster downloads)
- `noarch/repodata.json.zst` - Zstandard compressed (best compression)

3. Use the channel locally:
```bash
python -m http.server 8000
conda install -c http://localhost:8000 package-name
```

## GitHub Releases Hosting

When you push changes to `packages.txt`, GitHub Actions will automatically:
- Set up the conda environment from `environment.yml`
- Generate repodata with compression
- Upload to GitHub Releases (tag: `noarch`)

Use the channel from GitHub:
```bash
conda install -c https://github.com/danyeaw/dev-channel/releases/download/noarch package-name
```
