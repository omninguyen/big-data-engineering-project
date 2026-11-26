"""Entry point of the Databricks Python tasks.

  python pipeline.py [--set KEY=VALUE ...] ingest --source tmdb|musicbrainz [--date YYYY-MM-DD]

ingest   one batch of a source -> the ADLS container bronze (src/ingest): data files plus a manifest with the batch
         id, times, counts and status. --date is the logical date (folder dt=), default today (UTC).
--set    settings of src/common/config.py, applied before it is imported. Serverless job tasks have no cluster
         environment variables, so the job passes them as parameters, e.g. --set UC_CATALOG=big_data_engineering
         --set JOB_RUN_ID={{job.run_id}} --set TASK_RUN_ID={{task.run_id}}

A FAILED batch exits non-zero, so Databricks retries the task (as a new batch). PARTIAL (some pages failed) and
SKIPPED (no TMDB API key) batches succeed; their manifest tells them apart.
"""
import argparse
import os
import sys
from datetime import date, datetime, timezone

# Databricks script tasks may not define __file__; their working directory is this file's folder (DBR 14+).
try:
    _SRC = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _SRC = os.getcwd()
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)


def _date(value: str) -> str:
    """YYYY-MM-DD (it ends up in a folder name), or today in UTC when empty."""
    return date.fromisoformat(value).isoformat() if value else datetime.now(timezone.utc).date().isoformat()


def _setting(value: str):
    key, sep, val = value.partition("=")
    if not sep or not key.isidentifier() or not key.isupper():
        raise argparse.ArgumentTypeError(f"expected KEY=VALUE with an upper-case KEY, got {value!r}")
    return key, val


def ingest(args) -> dict:
    from ingest import runner
    return runner.run(args.source, args.date)


TASKS = {"ingest": ingest}


def parser() -> argparse.ArgumentParser:
    from ingest.sources import NAMES
    p = argparse.ArgumentParser(prog="pipeline.py", description="Databricks Python tasks: ingest")
    p.add_argument("--set", dest="settings", type=_setting, action="append", default=[], metavar="KEY=VALUE",
                   help="environment setting for src/common/config.py, repeatable")
    sub = p.add_subparsers(dest="task", required=True)
    s = sub.add_parser("ingest", help="one batch of a source -> bronze (JSONL + manifest)")
    s.add_argument("--source", choices=NAMES, required=True)
    s.add_argument("--date", type=_date, default="", help="logical date YYYY-MM-DD (default today, UTC)")
    return p


def main(argv=None):
    args = parser().parse_args(argv)
    for key, value in args.settings:  # before anything imports common.config
        os.environ[key] = value
    from common import logs
    logs.setup()
    return TASKS[args.task](args)


if __name__ == "__main__":
    main()
