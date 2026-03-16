# conda-pypi-test

A simple development conda channel for testing repodata for conda-pypi.

> [!WARNING]
> This is for testing new repodata formats and it is not for production use!
> If you are looking for production index generation use [conda-index](https://github.com/conda/conda-index).

## Quickstart for Using the Channel with conda

### Install the conda-pypi plugin

This will install the conda-pypi plugin and its dependencies:

```bash
conda install -n base 'conda-pypi>=0.5.0' 'conda-rattler-solver>=0.6.0'
```

### Configure the solver

The rattler solver is required for wheel installation:

```bash
conda config --set solver rattler
```

### Add the conda-pypi-test channel to your `.condarc`

This temporary test channel indexes approximately 500K pure Python wheels, latest version only:

```bash
conda config --append channels https://github.com/conda-incubator/conda-pypi-test/releases/download
```

## Setup

```bash
conda env create --file environment.yml
conda activate conda-pypi-test
```

## Generating the Test Channel

### Discover packages from PyPI

```bash
# 1. Get all package names (~739k, one request)
python discover.py --names-only --output names.txt

# 2. Add latest version, keep only packages with wheels (one request per name)
python discover.py --from-names names.txt --output packages.txt

# 3. Generate repodata
python generate.py
```

Step 2 can take a long time for the full list. Use `--workers` (default 50) and `--delay` to tune; if you hit rate limits (429), try `--workers 20 --delay 0.1`. Use `--any-wheel` to allow compiled wheels, not only pure Python.

### Manual package list

Edit `packages.txt` (format: `name==version` per line), then:

```bash
python generate.py
```

This creates `noarch/repodata.json` and `.zst`. Use the channel locally with `conda install -c . package-name`.

## GitHub Releases

Pushing to the repo triggers GitHub Actions to build and upload repodata. Use the channel with:

```bash
conda install -c https://github.com/conda-incubator/conda-pypi-test/releases/download/noarch package-name
```
