#!/usr/bin/env python3
"""
End-to-end test: resolve variant wheels via conda's solver.

Serves the generated variant repodata as a local channel, then uses
conda-rattler-solver to resolve torch with virtual package overrides
simulating a Linux system with CUDA 12.9 and GPU SM arch 9.0.

Prerequisites:
    - Run `python generate_variants.py` first to produce repodata
    - Run inside conda-pypi's pixi env:
      cd /path/to/conda-pypi && pixi run -e default python /path/to/this/script.py

Usage:
    python test_e2e_solve.py
"""

import os
import sys
import tempfile
import threading
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path

# Simulate a Linux x86_64 system with CUDA
os.environ["CONDA_SUBDIR"] = "linux-64"
os.environ["CONDA_OVERRIDE_CUDA"] = "12.9"
os.environ["CONDA_OVERRIDE_CUDA_ARCH"] = "9.0"
os.environ["CONDA_OVERRIDE_GLIBC"] = "2.35"
os.environ["CONDA_OVERRIDE_LINUX"] = "6.1"

from conda.base.context import reset_context
from conda.models.channel import Channel
from conda.models.match_spec import MatchSpec
from conda_rattler_solver.solver import RattlerSolver


def serve_channel(channel_root: Path) -> tuple[HTTPServer, str]:
    """Start a local HTTP server for the channel directory."""

    class Handler(SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=str(channel_root), **kwargs)

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, f"http://127.0.0.1:{port}"


def main():
    channel_root = Path(__file__).parent
    repodata = channel_root / "linux-64" / "repodata.json"

    if not repodata.exists():
        print("ERROR: linux-64/repodata.json not found.")
        print("       Run `python generate_variants.py` first.")
        return 1

    server, channel_url = serve_channel(channel_root)
    print(f"Serving variant channel at {channel_url}")
    print(f"Simulated system: linux-64, CUDA 12.9, SM 9.0, glibc 2.35\n")

    reset_context()

    prefix = Path(tempfile.mkdtemp()) / "env"
    solver = RattlerSolver(
        prefix=str(prefix),
        channels=[Channel(channel_url), Channel("conda-forge")],
        subdirs=["linux-64", "noarch"],
        specs_to_add=[MatchSpec("torch"), MatchSpec("python=3.12")],
    )

    print("Solving: torch + python=3.12")
    print(f"Channels: {channel_url} (variants), conda-forge (runtime deps)\n")

    try:
        to_unlink, to_link = solver.solve_for_diff()
    except Exception as e:
        print(f"FAILED: {type(e).__name__}: {e}")
        server.shutdown()
        return 1

    print(f"Resolved {len(to_link)} packages:\n")

    torch_rec = None
    for rec in sorted(to_link, key=lambda r: r.name):
        marker = ""
        if rec.name == "torch":
            torch_rec = rec
            marker = " <-- variant wheel"
        elif rec.name == "cuda-toolkit":
            marker = " <-- runtime dep from conda-forge"
        elif rec.name == "python":
            marker = " <-- from conda-forge"

        if marker:
            print(f"  {rec.name}-{rec.version} (build={rec.build}){marker}")

    if torch_rec:
        print(f"\ntorch details:")
        print(f"  url:     {getattr(torch_rec, 'url', 'n/a')}")
        print(f"  build:   {torch_rec.build}")
        print(f"  depends: {list(torch_rec.depends)}")
        print(f"  subdir:  {getattr(torch_rec, 'subdir', 'n/a')}")

    print("\nSUCCESS")
    server.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
