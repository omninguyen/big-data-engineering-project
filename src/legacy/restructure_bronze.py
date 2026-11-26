"""Move bronze files of the old layout into batch_id folders (one-off, run on Databricks).

  old  <root>/<source>/<entity>/dt=<date>/<file>.jsonl
  new  <root>/<source>/<entity>/dt=<date>/batch_id=<UTC start>-legacy/<file>.jsonl

The batch id comes from _ingested_at of the first record (the start of the old run). Empty files (skipped.jsonl)
use the time the file was last modified. File contents are not changed.

  python restructure_bronze.py [--root /Volumes/<catalog>/bronze/landing]            dry run: prints the plan
  python restructure_bronze.py [--root /Volumes/<catalog>/bronze/landing] --apply    moves the files

Each file is copied, compared with the original, then the original is deleted. Running it again is safe: files
already in a batch_id folder are left alone, and a copy left by an interrupted run is checked before the original
is deleted. Other files (manual/, unknown names) are listed but never touched. Exit code 1 if there was a conflict.
"""
import argparse
import filecmp
import json
import os
import shutil
import sys
from datetime import datetime, timezone

DEFAULT_ROOT = "/Volumes/big_data_engineering/bronze/landing"
ENTITIES = ["tmdb/movies", "musicbrainz/release_groups"]  # folders written by the old ingest code
SUFFIX = "legacy"


def legacy_batch_id(path: str) -> str:
    """<start time, UTC>-legacy, e.g. 20261124T013000Z-legacy (same shape as src/ingest/batch.py)."""
    started = None
    with open(path, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                value = json.loads(line).get("_ingested_at")
                if value:
                    started = datetime.fromisoformat(value.replace("Z", "+00:00"))
                break
    if started is None:  # empty file, or no _ingested_at
        started = datetime.fromtimestamp(os.path.getmtime(path), timezone.utc)
    if started.tzinfo is None:
        started = started.replace(tzinfo=timezone.utc)
    return f"{started.astimezone(timezone.utc):%Y%m%dT%H%M%SZ}-{SUFFIX}"


def plan(root: str):
    """(moves, ignored): moves are (source path, target path); ignored are paths left alone."""
    moves, ignored = [], []
    for entity in ENTITIES:
        base = os.path.join(root, entity)
        if not os.path.isdir(base):
            continue
        for dt in sorted(os.listdir(base)):
            folder = os.path.join(base, dt)
            if not (dt.startswith("dt=") and os.path.isdir(folder)):
                ignored.append(folder)
                continue
            for name in sorted(os.listdir(folder)):
                path = os.path.join(folder, name)
                if os.path.isdir(path):  # batch_id=... folders: already the new layout
                    continue
                if not name.endswith(".jsonl"):
                    ignored.append(path)
                    continue
                moves.append((path, os.path.join(folder, f"batch_id={legacy_batch_id(path)}", name)))
    return moves, ignored


def check(source: str, target: str) -> str:
    """'move', 'resume' (identical copy already there) or 'conflict' (a different file there)."""
    if not os.path.exists(target):
        return "move"
    return "resume" if filecmp.cmp(source, target, shallow=False) else "conflict"


def move(source: str, target: str) -> str:
    """'moved', 'resumed' (identical copy already there) or 'conflict' (different file there: nothing done)."""
    state = check(source, target)
    if state == "conflict":
        return "conflict"
    if state == "resume":
        os.remove(source)
        return "resumed"
    os.makedirs(os.path.dirname(target), exist_ok=True)
    shutil.copyfile(source, target)
    if not filecmp.cmp(source, target, shallow=False):
        os.remove(target)
        raise OSError(f"copy of {source} differs from the original, nothing moved")
    os.remove(source)
    return "moved"


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--root", default=DEFAULT_ROOT, help=f"landing volume (default {DEFAULT_ROOT})")
    p.add_argument("--apply", action="store_true", help="move the files (default: only print the plan)")
    args = p.parse_args(argv)
    root = args.root.rstrip("/")
    if not os.path.isdir(root):
        print(f"{root} not found")
        return 1

    moves, ignored = plan(root)
    conflicts = 0
    for source, target in moves:
        rel = os.path.relpath(source, root), os.path.relpath(target, root)
        result = move(source, target) if args.apply else {"move": "would move", "resume": "would resume",
                                                          "conflict": "conflict"}[check(source, target)]
        conflicts += result == "conflict"
        print(f"{result:<10}  {rel[0]} -> {rel[1]}")
    for path in ignored:
        print(f"left alone  {os.path.relpath(path, root)}")

    done = "moved" if args.apply else "to move (dry run, add --apply)"
    print(f"{len(moves) - conflicts} files {done}, {conflicts} conflicts, {len(ignored)} left alone")
    return 1 if conflicts else 0


if __name__ == "__main__":
    sys.exit(main())
