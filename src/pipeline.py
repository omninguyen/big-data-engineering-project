"""Ingest for Databricks: source API -> landing volume (JSONL), one Python task per source.

  python pipeline.py [--set KEY=VALUE ...] ingest --source tmdb|musicbrainz [--date YYYY-MM-DD]

ingest     TMDB movies -> <landing>/tmdb/movies/dt=<date>/, MusicBrainz albums -> <landing>/musicbrainz/release_groups/
           dt=<date>/ (landing: UC_LANDING_ROOT, default /Volumes/<UC_CATALOG>/bronze/landing)
--set      settings of src/common/config.py, applied before it is imported. Serverless job tasks have no cluster
           environment variables, so the job passes them as parameters, e.g. --set UC_CATALOG=big_data_engineering

--date defaults to today (UTC). A non-zero exit fails the task, so Databricks retries it. TMDB without an API key
(secret tmdb-api-key) is the one expected gap: it is logged and the task succeeds (Databricks has no 'skipped' state).
"""
import argparse
import logging
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

log = logging.getLogger("pipeline")


def _date(value: str) -> str:
    """YYYY-MM-DD (it ends up in a folder name), or today in UTC when empty."""
    return date.fromisoformat(value).isoformat() if value else datetime.now(timezone.utc).date().isoformat()


def _setting(value: str):
    key, sep, val = value.partition("=")
    if not sep or not key.isidentifier() or not key.isupper():
        raise argparse.ArgumentTypeError(f"expected KEY=VALUE with an upper-case KEY, got {value!r}")
    return key, val


def ingest(args) -> int:
    from common import config
    if args.source == "musicbrainz":
        from ingest import musicbrainz
        return musicbrainz.run(args.date)
    if config.MOVIE_SOURCE == "file":  # TMDB export uploaded to MOVIE_FILE in the landing volume
        from ingest import tmdb_file
        return tmdb_file.run(args.date)
    if config.MOVIE_SOURCE == "sample":  # built-in demo set, no network needed
        from ingest import sample_movies
        return sample_movies.run(args.date)
    from ingest import tmdb
    from common.errors import MissingCredentials
    try:
        return tmdb.run(args.date)
    except MissingCredentials as e:
        from common import storage
        # An empty file keeps tmdb/movies/ in place: a later read_files of that folder fails when it is missing.
        # A separate name never overwrites a real delivery of the same day.
        path = storage.put_bytes(f"bronze/tmdb/movies/dt={args.date}/skipped.jsonl", b"")
        log.warning("Skipping TMDB ingest: %s (wrote empty %s)", e, path)
        return 0


TASKS = {"ingest": ingest}


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="pipeline.py", description="Ingest for Databricks: source API -> landing volume")
    p.add_argument("--set", dest="settings", type=_setting, action="append", default=[], metavar="KEY=VALUE",
                   help="environment setting for src/common/config.py, repeatable")
    sub = p.add_subparsers(dest="task", required=True)
    s = sub.add_parser("ingest", help="source API -> landing volume (bronze JSONL)")
    s.add_argument("--source", choices=["tmdb", "musicbrainz"], required=True)
    s.add_argument("--date", type=_date, default="")
    return p


def main(argv=None):
    args = parser().parse_args(argv)
    for key, value in args.settings:  # before anything imports common.config
        os.environ[key] = value
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    result = TASKS[args.task](args)
    log.info("%s finished: %s", args.task, result)
    return result


if __name__ == "__main__":
    main()
