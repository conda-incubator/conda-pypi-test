# conda-pypi-test

A simple development conda channel for testing repodata for conda-pypi.

> [!WARNING]
> This is for testing new repodata formats and it is not for production use!
> If you are looking for production index generation for a channel use [conda-index](https://github.com/conda/conda-index)).

## Setup

Create the conda environment:

```bash
conda env create -f environment.yml
conda activate conda-pypi-test
```

## Quick Start

1. Edit `packages.txt` with your desired packages:

```
requests==2.32.5
fastapi==0.116.1
```

1. Generate repodata:

```bash
python generate.py
```

This generates:

- `noarch/repodata.json` - Uncompressed metadata
- `noarch/repodata.json.bz2` - Bzip2 compressed (faster downloads)
- `noarch/repodata.json.zst` - Zstandard compressed (best compression)

1. Use the channel locally:

```bash
conda install -c . package-name
```

## GitHub Releases Hosting

When you push changes to `packages.txt`, GitHub Actions will automatically:

- Set up the conda environment from `environment.yml`
- Generate repodata with compression
- Upload to GitHub Releases (tag: `noarch`)

Use the channel from GitHub:

```bash
conda install -c https://github.com/conda-incubator/conda-pypi-test/releases/download/noarch package-name
```

