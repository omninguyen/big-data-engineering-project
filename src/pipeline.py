"""Ingest for Databricks: source API -> landing/ of the ADLS container bronze (JSONL), one Python task per source,
then the bronze and silver tables in Unity Catalog.

  python pipeline.py [--set KEY=VALUE ...] ingest --source tmdb|musicbrainz [--date YYYY-MM-DD]
  python pipeline.py [--set KEY=VALUE ...] bronze
  python pipeline.py [--set KEY=VALUE ...] silver

ingest     TMDB movies -> <bronze>/tmdb/movies/dt=<date>/batch_id=<batch id>/, MusicBrainz albums ->
           <bronze>/musicbrainz/release_groups/dt=<date>/batch_id=<batch id>/
           (bronze: BRONZE_ROOT, e.g. abfss://bronze@<account>.dfs.core.windows.net/landing). Every run is a new batch
           (src/ingest/batch.py), so a rerun of the same --date never overwrites an earlier one.
bronze     creates the bronze tables if missing (src/sql/bronze/): external Delta tables in tables/, next to
           landing/ (BRONZE_ROOT), then loads the new files of landing/ into them with COPY INTO
           (src/sql/bronze/load/). Runs after the ingest tasks.
silver     creates the silver tables if missing (src/sql/silver/): external Delta tables in the container silver
           (SILVER_ROOT), then loads every new bronze batch into them, oldest first (src/sql/silver/load/).
           Runs after bronze.
--set      settings of src/common/config.py, applied before it is imported. Serverless job tasks have no cluster
           environment variables, so the job passes them as parameters, e.g.
           --set BRONZE_ROOT=abfss://bronze@<account>.dfs.core.windows.net/landing

--date defaults to today in Vietnam time (UTC+7, the timezone of the job's schedule), as the batch id
(src/ingest/batch.py): the 02:00 run on 27/11 writes dt=2026-11-27/batch_id=20261127T020005-<6 hex>.
A non-zero exit fails the task, so Databricks retries it. TMDB without an API key
(secret tmdb-api-key) is the one expected gap: it is logged and the task succeeds (Databricks has no 'skipped' state).
"""
import argparse
import logging
import os
import sys
from datetime import date

# Databricks script tasks may not define __file__; their working directory is this file's folder (DBR 14+).
try:
    _SRC = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _SRC = os.getcwd()
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

log = logging.getLogger("pipeline")


def _date(value: str) -> str:
    """YYYY-MM-DD (it ends up in a folder name), or today in Vietnam time when empty."""
    from ingest.batch import today
    return date.fromisoformat(value).isoformat() if value else today()


def _setting(value: str):
    key, sep, val = value.partition("=")
    if not sep or not key.isidentifier() or not key.isupper():
        raise argparse.ArgumentTypeError(f"expected KEY=VALUE with an upper-case KEY, got {value!r}")
    return key, val


def ingest(args) -> int:
    from common import config
    from ingest.batch import bronze_key, new_batch_id
    batch_id = new_batch_id()
    log.info("%s ingest for dt=%s: batch %s", args.source, args.date, batch_id)
    if args.source == "musicbrainz":
        from ingest import musicbrainz
        return musicbrainz.run(args.date, batch_id)
    if config.MOVIE_SOURCE == "file":  # TMDB export uploaded to MOVIE_FILE, by default <bronze>/manual/
        from ingest import tmdb_file
        return tmdb_file.run(args.date, batch_id)
    if config.MOVIE_SOURCE == "sample":  # built-in demo set, no network needed
        from ingest import sample_movies
        return sample_movies.run(args.date, batch_id)
    from ingest import tmdb
    from common.errors import MissingCredentials
    try:
        return tmdb.run(args.date, batch_id)
    except MissingCredentials as e:
        from common import storage
        # An empty file keeps tmdb/movies/ in place: a later read_files of that folder fails when it is missing.
        path = storage.put_bytes(bronze_key("tmdb", "movies", args.date, batch_id, "skipped.jsonl"), b"")
        log.warning("Skipping TMDB ingest: %s (wrote empty %s)", e, path)
        return 0


def _spark():
    from pyspark.sql import SparkSession  # preinstalled on Databricks
    return SparkSession.builder.getOrCreate()


def bronze(args) -> dict:
    import sql
    spark = _spark()
    sql.create_bronze_tables(spark)
    return sql.load_bronze_tables(spark)


def silver(args) -> dict:
    import sql
    spark = _spark()
    sql.create_silver_tables(spark)
    return sql.load_silver_tables(spark)


TASKS = {"ingest": ingest, "bronze": bronze, "silver": silver}


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="pipeline.py", description="Ingest for Databricks: source API -> ADLS bronze")
    p.add_argument("--set", dest="settings", type=_setting, action="append", default=[], metavar="KEY=VALUE",
                   help="environment setting for src/common/config.py, repeatable")
    sub = p.add_subparsers(dest="task", required=True)
    s = sub.add_parser("ingest", help="source API -> landing/ of the ADLS container bronze (JSONL)")
    s.add_argument("--source", choices=["tmdb", "musicbrainz"], required=True)
    s.add_argument("--date", type=_date, default="")
    sub.add_parser("bronze", help="create the bronze tables in Unity Catalog if missing and load landing/ into them")
    sub.add_parser("silver", help="create the silver tables if missing and load the new bronze batches into them")
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
