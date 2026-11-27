"""Ingest for Databricks: source API -> landing/ of the ADLS container bronze (JSONL), one Python task per source,
then the bronze, silver and gold tables in Unity Catalog, and the ops tables that monitor them.

  python pipeline.py [--set KEY=VALUE ...] ingest --source tmdb|musicbrainz|listenbrainz [--date YYYY-MM-DD]
  python pipeline.py [--set KEY=VALUE ...] setup
  python pipeline.py [--set KEY=VALUE ...] bronze [--table <bronze table>]
  python pipeline.py [--set KEY=VALUE ...] silver [--table <silver table>]
  python pipeline.py [--set KEY=VALUE ...] gold [--table <gold table>]
  python pipeline.py [--set KEY=VALUE ...] ops [--table <ops table>]
  python pipeline.py [--set KEY=VALUE ...] marts

ingest     TMDB movies -> <bronze>/tmdb/movies/dt=<date>/batch_id=<batch id>/, MusicBrainz albums ->
           <bronze>/musicbrainz/release_groups/dt=<date>/batch_id=<batch id>/, ListenBrainz listen counts of the
           albums in silver.release_group -> <bronze>/listenbrainz/release_group_popularity/dt=<date>/batch_id=<id>/
           (bronze: BRONZE_ROOT, e.g. abfss://bronze@<account>.dfs.core.windows.net/landing). Every run is a new batch
           (src/ingest/batch.py), so a rerun of the same --date never overwrites an earlier one.
setup      creates the bronze, silver, gold and ops tables if missing (src/sql/bronze/, silver/, gold/, ops/):
           external Delta tables in tables/ next to landing/ (BRONZE_ROOT), in the container silver (SILVER_ROOT),
           in the container gold (GOLD_ROOT) and in its folder ops/.
bronze     loads the new files of landing/ into a bronze table with COPY INTO (src/sql/bronze/load/).
silver     loads a silver table (src/sql/silver/load/): every new bronze batch, oldest first, for an incremental
           table; a rebuild for a full-load table.
gold       rebuilds a gold table from silver (src/sql/gold/load/), on every run.
ops        rebuilds an ops table (src/sql/ops/load/): rows, files, lag and quality checks of every ingest batch.
marts      creates or replaces the views of the marts schema (src/sql/marts/), read by the dashboard (Preset).
           Without --table, each command loads every table of its layer. The job runs one task per table
           (databricks.yml), so the data path is visible in the job graph.
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
    if args.source == "listenbrainz":  # popularity of the albums in silver.release_group
        from ingest import listenbrainz
        return listenbrainz.run(args.date, batch_id)
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


def setup(args) -> list:
    import sql
    return sql.create_tables(_spark())


def bronze(args) -> dict:
    import sql
    spark = _spark()
    return sql.load_bronze_table(spark, args.table) if args.table else sql.load_bronze_tables(spark)


def silver(args) -> dict:
    import sql
    spark = _spark()
    return sql.load_silver_table(spark, args.table) if args.table else sql.load_silver_tables(spark)


def gold(args) -> dict:
    import sql
    spark = _spark()
    return sql.load_gold_table(spark, args.table) if args.table else sql.load_gold_tables(spark)


def ops(args) -> dict:
    import sql
    spark = _spark()
    return sql.load_ops_table(spark, args.table) if args.table else sql.load_ops_tables(spark)


def marts(args) -> list:
    import sql
    return sql.create_marts_views(_spark())


TASKS = {"ingest": ingest, "setup": setup, "bronze": bronze, "silver": silver, "gold": gold, "ops": ops,
         "marts": marts}


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="pipeline.py", description="Ingest for Databricks: source API -> ADLS bronze")
    p.add_argument("--set", dest="settings", type=_setting, action="append", default=[], metavar="KEY=VALUE",
                   help="environment setting for src/common/config.py, repeatable")
    sub = p.add_subparsers(dest="task", required=True)
    s = sub.add_parser("ingest", help="source API -> landing/ of the ADLS container bronze (JSONL)")
    s.add_argument("--source", choices=["tmdb", "musicbrainz", "listenbrainz"], required=True)
    s.add_argument("--date", type=_date, default="")
    import sql
    sub.add_parser("setup", help="create the bronze, silver, gold and ops tables in Unity Catalog if missing")
    s = sub.add_parser("bronze", help="load the new files of landing/ into a bronze table (default: all)")
    s.add_argument("--table", choices=sql.BRONZE_TABLES)
    s = sub.add_parser("silver", help="load a silver table from bronze (default: all)")
    s.add_argument("--table", choices=sql.SILVER_LOADED)
    s = sub.add_parser("gold", help="rebuild a gold table from silver (default: all)")
    s.add_argument("--table", choices=sql.GOLD_TABLES)
    s = sub.add_parser("ops", help="rebuild an ops table: pipeline metrics and data quality (default: all)")
    s.add_argument("--table", choices=sql.OPS_TABLES)
    sub.add_parser("marts", help="create or replace the views of the marts schema, read by the dashboard")
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
