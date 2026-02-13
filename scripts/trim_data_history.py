"""Trim old daily data folders and keep only recent snapshots.

Usage:
    python scripts/trim_data_history.py --keep-days 21
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path


def parse_args() -> argparse.Namespace:
    """Parse CLI options."""
    parser = argparse.ArgumentParser(description="Trim old data snapshots.")
    parser.add_argument(
        "--keep-days",
        type=int,
        default=21,
        help="How many dated folders to keep in data/ (default: 21).",
    )
    return parser.parse_args()


def is_date_folder(name: str) -> bool:
    """Return True when folder looks like YYYY-MM-DD."""
    return len(name) == 10 and name[4] == "-" and name[7] == "-" and name.replace("-", "").isdigit()


def trim_data_history(project_root: Path, keep_days: int) -> int:
    """Delete old date folders beyond keep_days. Returns deleted count."""
    data_dir = project_root / "data"
    if not data_dir.exists():
        return 0

    dated_dirs = sorted(
        [item for item in data_dir.iterdir() if item.is_dir() and is_date_folder(item.name)],
        key=lambda item: item.name,
    )

    if keep_days < 1:
        keep_days = 1

    to_delete = dated_dirs[:-keep_days]
    for folder in to_delete:
        shutil.rmtree(folder, ignore_errors=True)

    return len(to_delete)


def main() -> int:
    """CLI entrypoint."""
    args = parse_args()
    project_root = Path(__file__).resolve().parent.parent
    deleted_count = trim_data_history(project_root=project_root, keep_days=args.keep_days)
    print(f"Deleted {deleted_count} old data folder(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

