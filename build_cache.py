#!/usr/bin/env python3
"""Build a local SQLite cache of package metadata from packages.txt."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import sqlite3
from pathlib import Path

import httpx
from hishel import SyncSqliteStorage
from hishel.httpx import SyncCacheTransport
from unearth import PackageFinder

# Do not use unearth.fetchers.pypiclient
from unearth.fetchers.sync import PyPIClient

PYPI_SIMPLE_INDEX_URL = "https://pypi.org/simple/"


def load_requirements(path: Path) -> list[str]:
    """Read package requirements from a plain text file."""
    requirements: list[str] = []
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            requirements.append(line)
    return requirements


def create_results_db(path: Path) -> sqlite3.Connection:
    """Create/open the sqlite cache database and ensure schema exists."""
    connection = sqlite3.connect(path)
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS cache (
            url TEXT PRIMARY KEY,
            name TEXT,
            version TEXT,
            metadata_text TEXT
        )
        """
    )
    return connection


def create_finder(cache_db_path: Path) -> PackageFinder:
    """Create a PackageFinder whose HTTP requests are cached by hishel."""
    # Finder requests run in threadpool workers, so disable sqlite thread affinity.
    cache_connection = sqlite3.connect(cache_db_path, check_same_thread=False)
    storage = SyncSqliteStorage(
        connection=cache_connection,
        database_path=cache_db_path,
    )
    mounts = {
        "http://": SyncCacheTransport(httpx.HTTPTransport(), storage=storage),
        "https://": SyncCacheTransport(httpx.HTTPTransport(), storage=storage),
    }
    client = PyPIClient(mounts=mounts)
    return PackageFinder(session=client, index_urls=[PYPI_SIMPLE_INDEX_URL])


def resolve_requirement(
    requirement: str, finder: PackageFinder
) -> tuple[str, str, str, str] | None:
    """Resolve one requirement and return a DB row: (url, name, version, metadata_text)."""
    try:
        match = finder.find_best_match(requirement)
        if match.best is None:
            return None

        package = match.best
        link = package.link
        metadata_text = ""
        if link.dist_info_link is not None:
            response = finder.session.get(link.dist_info_link.url_without_fragment)
            response.raise_for_status()
            metadata_text = response.content.decode("utf-8", errors="replace")

        return (
            link.url_without_fragment,
            package.name,
            str(package.version) if package.version is not None else "",
            metadata_text,
        )
    except Exception:
        return None


async def process_requirements(
    requirements: list[str],
    cache_db_path: Path,
    output_db_path: Path,
    concurrency: int,
) -> tuple[int, int]:
    """Resolve requirements concurrently and write rows into sqlite."""
    if concurrency < 1:
        raise ValueError("concurrency must be >= 1")

    connection = create_results_db(output_db_path)
    finder = create_finder(cache_db_path)
    lock = asyncio.Lock()
    semaphore = asyncio.Semaphore(concurrency)
    found = 0
    missing = 0
    completed = 0
    total = len(requirements)
    in_progress: set[str] = set()

    async def report_progress() -> None:
        while True:
            await asyncio.sleep(5)
            async with lock:
                current = next(iter(in_progress), None)
                if current is None:
                    print(
                        f"[progress] completed={completed}/{total}, current=(idle)",
                        flush=True,
                    )
                else:
                    print(
                        f"[progress] completed={completed}/{total}, current={current}",
                        flush=True,
                    )

    async def worker(requirement: str) -> None:
        nonlocal found, missing, completed
        async with semaphore:
            async with lock:
                in_progress.add(requirement)
            row = await asyncio.to_thread(resolve_requirement, requirement, finder)
            async with lock:
                in_progress.discard(requirement)
                completed += 1
                if row is None:
                    missing += 1
                    print(
                        f"[done] {completed}/{total} {requirement} -> no match",
                        flush=True,
                    )
                    return
                connection.execute(
                    """
                    INSERT INTO cache(url, name, version, metadata_text)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(url) DO UPDATE SET
                        name=excluded.name,
                        version=excluded.version,
                        metadata_text=excluded.metadata_text
                    """,
                    row,
                )
                found += 1
                print(
                    f"[done] {completed}/{total} {requirement} -> cached",
                    flush=True,
                )

    progress_task = asyncio.create_task(report_progress())
    try:
        await asyncio.gather(*(worker(req) for req in requirements))
        progress_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await progress_task
        connection.commit()
    finally:
        if not progress_task.done():
            progress_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await progress_task
        finder.session.close()
        connection.close()

    return found, missing


async def async_main() -> None:
    parser = argparse.ArgumentParser(
        description="Resolve packages with unearth and cache metadata text in sqlite.",
    )
    parser.add_argument(
        "--packages",
        type=Path,
        default=Path("packages.txt"),
        help="Input requirements file (default: packages.txt).",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=Path("build_cache.sqlite3"),
        help="Output sqlite database (default: build_cache.sqlite3).",
    )
    parser.add_argument(
        "--http-cache-db",
        type=Path,
        default=Path("hishel_http_cache.sqlite3"),
        help="hishel cache sqlite for HTTP responses (default: hishel_http_cache.sqlite3).",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=20,
        help="Number of concurrent requirement lookups (default: 20).",
    )
    args = parser.parse_args()

    requirements = load_requirements(args.packages)
    total = len(requirements)
    if total == 0:
        print(f"No package requirements found in {args.packages}")
        return

    found, missing = await process_requirements(
        requirements=requirements,
        cache_db_path=args.http_cache_db,
        output_db_path=args.db,
        concurrency=args.concurrency,
    )
    print(f"Processed {total} requirements")
    print(f"Cached {found} records in {args.db}")
    print(f"No match for {missing} requirements")


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
