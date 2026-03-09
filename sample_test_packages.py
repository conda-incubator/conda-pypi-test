#!/usr/bin/env python3
"""Pick random package lines from packages.txt and write test-packages.txt."""

from __future__ import annotations

import random
from pathlib import Path


def main() -> None:
    source = Path("packages.txt")
    target = Path("test-packages.txt")

    lines = [
        line.strip()
        for line in source.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    count = min(128, len(lines))
    sample = random.sample(lines, k=count)
    target.write_text("\n".join(sample) + "\n", encoding="utf-8")
    print(f"Wrote {count} lines to {target}")


if __name__ == "__main__":
    main()
