#!/usr/bin/env python3
"""Build a local SQLite cache of package metadata from packages.txt."""

from __future__ import annotations

import argparse
import contextlib
import queue
import random
import sqlite3
import threading
import time
from pathlib import Path
from typing import Callable, cast

import httpx
from hishel import SyncSqliteStorage
from hishel.httpx import SyncCacheTransport
from unearth import PackageFinder
from unearth.fetchers import Fetcher
from unearth.fetchers.sync import PyPIClient

from unearth_fetcher import SharedAsyncPyPIClient

PYPI_SIMPLE_INDEX_URL = "https://pypi.org/simple/"
ResultRow = tuple[str, str, str, str] | None
ResultItem = tuple[str, ResultRow]
CloseFetcher = Callable[[], None]


def report_progress(message: str) -> None:
    print(message, flush=True)


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


def create_finder(
    cache_db_path: Path,
    client: str,
    request_timeout: float,
) -> tuple[PackageFinder, CloseFetcher]:
    """Create a finder using either shared async or normal sync fetcher."""
    if client == "shared-async":
        try:
            async_client = SharedAsyncPyPIClient(
                http_cache_db_path=cache_db_path,
                timeout=request_timeout,
            )
        except ImportError as error:
            raise RuntimeError(
                "SharedAsyncPyPIClient requires hishel async sqlite support. "
                "Install dependencies with `pip install hishel[async] anysqlite`."
            ) from error
        finder = PackageFinder(
            session=cast(Fetcher, async_client),
            index_urls=[PYPI_SIMPLE_INDEX_URL],
        )
        return finder, async_client.close

    if client == "sync":
        cache_connection = sqlite3.connect(cache_db_path, check_same_thread=False)
        storage = SyncSqliteStorage(
            connection=cache_connection,
            database_path=cache_db_path,
        )
        mounts = {
            "http://": SyncCacheTransport(httpx.HTTPTransport(), storage=storage),
            "https://": SyncCacheTransport(httpx.HTTPTransport(), storage=storage),
        }
        sync_client = PyPIClient(mounts=mounts, timeout=request_timeout)
        finder = PackageFinder(
            session=cast(Fetcher, sync_client),
            index_urls=[PYPI_SIMPLE_INDEX_URL],
        )

        def close_sync_client() -> None:
            with contextlib.suppress(Exception):
                sync_client.close()
            with contextlib.suppress(Exception):
                cache_connection.close()

        return finder, close_sync_client

    raise ValueError(f"Unsupported client type: {client}")


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


def process_requirements(
    requirements: list[str],
    cache_db_path: Path,
    output_db_path: Path,
    client: str,
    concurrency: int,
    timeout_seconds: int,
) -> tuple[int, int, bool, int]:
    """Resolve requirements with worker threads and collect inserts in one thread."""
    if concurrency < 1:
        raise ValueError("concurrency must be >= 1")
    if timeout_seconds < 1:
        raise ValueError("timeout_seconds must be >= 1")

    total = len(requirements)
    start_time = time.monotonic()
    deadline = time.monotonic() + float(timeout_seconds)
    request_timeout = 10.0
    shutdown_grace_seconds = 1.0

    requirements_queue: queue.Queue[str] = queue.Queue()
    for requirement in requirements:
        requirements_queue.put(requirement)

    sentinel = object()
    results_queue: queue.Queue[ResultItem | object] = queue.Queue()

    state_lock = threading.Lock()
    in_progress: set[str] = set()
    found = 0
    missing = 0
    completed = 0
    timed_out = False

    stop_event = threading.Event()
    collector_done_event = threading.Event()

    finder, close_finder = create_finder(cache_db_path, client, request_timeout)
    collector_error: Exception | None = None

    def worker() -> None:
        nonlocal timed_out
        try:
            while not stop_event.is_set():
                if time.monotonic() >= deadline:
                    with state_lock:
                        timed_out = True
                    stop_event.set()
                    break

                try:
                    next_requirement = requirements_queue.get_nowait()
                except queue.Empty:
                    break

                with state_lock:
                    in_progress.add(next_requirement)

                row = resolve_requirement(next_requirement, finder)
                results_queue.put((next_requirement, row))
        finally:
            results_queue.put(sentinel)

    def collector() -> None:
        nonlocal found, missing, completed, collector_error
        done_workers = 0
        pending_writes = 0
        connection = create_results_db(output_db_path)
        try:
            while done_workers < concurrency:
                try:
                    item = results_queue.get(timeout=0.1)
                except queue.Empty:
                    continue

                if item is sentinel:
                    done_workers += 1
                    continue

                requirement, row = cast(ResultItem, item)
                with state_lock:
                    in_progress.discard(requirement)
                    completed += 1

                if row is None:
                    missing += 1
                    continue

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
                pending_writes += 1
                if pending_writes >= 100:
                    connection.commit()
                    pending_writes = 0

            connection.commit()
        except Exception as error:
            collector_error = error
            stop_event.set()
        finally:
            connection.close()
            collector_done_event.set()

    def progress_reporter() -> None:
        while not collector_done_event.is_set():
            time.sleep(5)
            with state_lock:
                remaining_seconds = max(0, int(deadline - time.monotonic()))
                elapsed = max(0.001, time.monotonic() - start_time)
                packages_per_second = completed / elapsed
                current = next(iter(in_progress), None)
                if current is None:
                    report_progress(
                        f"[progress] completed={completed}/{total}, rate={packages_per_second:.2f} pkg/s, current=(idle), remaining={remaining_seconds}s",
                    )
                else:
                    report_progress(
                        f"[progress] completed={completed}/{total}, rate={packages_per_second:.2f} pkg/s, current={current}, remaining={remaining_seconds}s",
                    )

    workers = [threading.Thread(target=worker, daemon=True) for _ in range(concurrency)]
    collector_thread = threading.Thread(target=collector, daemon=True)
    progress_thread = threading.Thread(target=progress_reporter, daemon=True)
    lingering_workers = False

    try:
        collector_thread.start()
        progress_thread.start()
        for thread in workers:
            thread.start()

        while any(thread.is_alive() for thread in workers):
            if time.monotonic() >= deadline:
                timed_out = True
                stop_event.set()
                break
            time.sleep(0.05)

        if timed_out:
            grace_deadline = time.monotonic() + shutdown_grace_seconds
            while any(thread.is_alive() for thread in workers):
                remaining = grace_deadline - time.monotonic()
                if remaining <= 0:
                    break
                time.sleep(min(0.05, remaining))
            collector_thread.join(timeout=max(0.0, grace_deadline - time.monotonic()))
            lingering_workers = any(thread.is_alive() for thread in workers)
        else:
            for thread in workers:
                thread.join()
            collector_thread.join()
    finally:
        stop_event.set()
        collector_done_event.set()
        progress_thread.join(timeout=1)
        if not lingering_workers:
            close_finder()

    if collector_error is not None:
        raise RuntimeError("collector thread failed") from collector_error

    skipped = total - completed
    return found, missing, timed_out, skipped


def main() -> None:
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
        "--client",
        choices=["shared-async", "sync"],
        default="shared-async",
        help="Fetcher type (default: shared-async).",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=32,
        help="Number of requirement worker threads (default: 32).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=300,
        help="Stop scheduling new lookups after this many seconds (default: 300).",
    )
    args = parser.parse_args()

    requirements = load_requirements(args.packages)
    random.shuffle(requirements)
    total = len(requirements)
    if total == 0:
        report_progress(f"No package requirements found in {args.packages}")
        return

    found, missing, timed_out, skipped = process_requirements(
        requirements=requirements,
        cache_db_path=args.http_cache_db,
        output_db_path=args.db,
        client=args.client,
        concurrency=args.concurrency,
        timeout_seconds=args.timeout,
    )
    completed = total - skipped
    report_progress(f"Completed {completed}/{total} requirements")
    report_progress(f"Cached {found} records in {args.db}")
    report_progress(f"No match for {missing} requirements")
    if timed_out:
        report_progress(f"Stopped after timeout of {args.timeout}s")
        report_progress(f"Unprocessed requirements: {skipped}")


if __name__ == "__main__":
    main()
