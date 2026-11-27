"""SQL of the Unity Catalog tables, run with spark.sql (pipeline.py setup, bronze, silver, gold, ops: a task per table).

src/sql/bronze/<table>.sql       creates <catalog>.bronze.<table>, an external Delta table in <tables root>/<table>
src/sql/bronze/load/<table>.sql  loads the new files of landing/ into it with COPY INTO
src/sql/silver/<table>.sql       creates <catalog>.silver.<table>, an external Delta table in <silver root>/<table>
src/sql/silver/load/<table>.sql  incremental tables: loads one bronze batch; run once per new batch, oldest first,
                                 and recorded in silver.load_log (the watermark: a recorded batch is not loaded again)
                                 full-load tables: rebuilds the whole table, on every run, after the incremental ones
src/sql/gold/<table>.sql         creates <catalog>.gold.<table>, an external Delta table in <gold root>/<table>
src/sql/gold/load/<table>.sql    rebuilds it from silver on every run (full load)
src/sql/ops/<table>.sql          creates <catalog>.ops.<table> (pipeline metrics, data quality) in <ops root>/<table>
src/sql/ops/load/<table>.sql     rebuilds it from bronze and silver.load_log on every run (full load)
src/sql/marts/<view>.sql         creates or replaces the view <catalog>.marts.<view> over gold and ops: one view per
                                 dataset of the dashboard (Preset), no data of its own
${...} placeholders are filled in from common/config.py (UC_CATALOG, BRONZE_TABLES_ROOT, BRONZE_ROOT, SILVER_ROOT,
GOLD_ROOT, OPS_ROOT),
so the files hold no workspace or storage details. A file may hold several statements, each ending with ';' at the
end of a line.
"""
import logging
import os
import re
from string import Template

log = logging.getLogger(__name__)
HERE = os.path.dirname(os.path.abspath(__file__))
BRONZE_TABLES = ["tmdb_movies", "tmdb_trending_movies", "musicbrainz_release_groups",
                 "listenbrainz_release_group_popularity", "wikidata_media_articles", "wikipedia_pageviews"]
SILVER_TABLES = ["load_log", "movie", "movie_metric_daily", "release_group", "artist", "release_group_metric_daily",
                 "genre", "movie_genre", "release_group_genre", "release_group_artist",  # genre before its foreign keys
                 "media_wiki_page", "wiki_pageview_daily", "movie_trending_daily"]
SILVER_INCREMENTAL = {  # silver table: its bronze table
    "movie": "tmdb_movies", "movie_metric_daily": "tmdb_movies",
    "release_group": "musicbrainz_release_groups", "artist": "musicbrainz_release_groups",
    "release_group_metric_daily": "listenbrainz_release_group_popularity",
    "media_wiki_page": "wikidata_media_articles", "wiki_pageview_daily": "wikipedia_pageviews",
    "movie_trending_daily": "tmdb_trending_movies"}
SILVER_FULL = ["genre", "movie_genre", "release_group_genre", "release_group_artist"]
SILVER_LOADED = [*SILVER_INCREMENTAL, *SILVER_FULL]  # every silver table with a load (load_log is written by them)
GOLD_TABLES = ["dim_date", "dim_media", "dim_genre", "bridge_media_genre",  # full loads; dims before their foreign keys
               "fact_media_metric_daily", "fact_media_change", "dim_wiki_page", "fact_wiki_pageview_daily",
               "fact_media_trending_daily"]
OPS_TABLES = ["pipeline_record_metrics", "data_quality_results"]  # full loads; data_quality_results reads the first
MARTS_VIEWS = ["catalog_items", "items_per_genre", "items_per_release_year", "movies_by_language", "top_titles",
               "daily_listens", "trending_7d", "new_releases", "catalog_changes",
               "wiki_attention", "daily_wiki_views", "wiki_coverage", "tmdb_trending_today",  # content, over gold
               "freshness", "latest_quality_checks", "quality_history", "data_catalog"]  # operations, over ops
BATCH_ID = re.compile(r"[0-9A-Za-z-]+")


def render(layer: str, name: str, **params: str) -> str:
    """The text of src/sql/<layer>/<name>.sql. A placeholder without a value raises KeyError."""
    with open(os.path.join(HERE, layer, f"{name}.sql"), encoding="utf-8") as f:
        return Template(f.read()).substitute(params)


def statements(text: str) -> list:
    """The statements of a SQL text, split at each ';' that ends a line. Parts with only comments are dropped."""
    parts = re.split(r";[ \t]*$", text, flags=re.M)
    return [p.strip() for p in parts if re.sub(r"--[^\n]*", "", p).strip()]


def _abfss(setting: str, value: str, example: str) -> str:
    """The value, if it is a plain abfss:// path (it goes inside a quoted SQL literal)."""
    if not re.fullmatch(r"abfss://[\w.@/=-]+", value):
        raise ValueError(f"{setting} must be an abfss:// path such as {example}, got {value!r}")
    return value


def _create(spark, layer: str, names: list, root: str, placeholder: str) -> list:
    """Create the tables of a layer if missing. A table of the same name somewhere else (e.g. a managed table)
    fails: CREATE TABLE IF NOT EXISTS would silently keep it."""
    from common import config
    tables = []
    for name in names:
        table, location = f"{config.UC_CATALOG}.{layer}.{name}", f"{root}/{name}"
        spark.sql(render(layer, name, catalog=config.UC_CATALOG, **{placeholder: root}))
        actual = spark.sql(f"DESCRIBE DETAIL {table}").first()["location"].rstrip("/")
        if actual != location:
            raise RuntimeError(f"{table} already exists in {actual}, not in {location}: "
                               "drop it (it may be a managed table) and run again")
        log.info("%s: external Delta table in %s", table, location)
        tables.append(table)
    return tables


def create_bronze_tables(spark) -> list:
    from common import config
    root = _abfss("BRONZE_TABLES_ROOT", config.BRONZE_TABLES_ROOT,
                  "abfss://bronze@<account>.dfs.core.windows.net/tables (tables/ next to BRONZE_ROOT=.../landing)")
    return _create(spark, "bronze", BRONZE_TABLES, root, "tables_root")


def load_bronze_table(spark, name: str) -> dict:
    """COPY INTO one bronze table from landing/. Returns the COPY INTO result, e.g. {"num_inserted_rows": 800}."""
    from common import config
    if name not in BRONZE_TABLES:
        raise ValueError(f"unknown bronze table {name!r}, expected one of {BRONZE_TABLES}")
    landing = _abfss("BRONZE_ROOT", config.BRONZE_ROOT, "abfss://bronze@<account>.dfs.core.windows.net/landing")
    row = spark.sql(render("bronze/load", name, catalog=config.UC_CATALOG, landing_root=landing)).first()
    result = row.asDict() if row is not None else {}
    log.info("%s.bronze.%s: COPY INTO %s", config.UC_CATALOG, name, result)
    return result


def load_bronze_tables(spark) -> dict:
    """COPY INTO every bronze table. Returns {table: COPY INTO result}."""
    from common import config
    return {f"{config.UC_CATALOG}.bronze.{name}": load_bronze_table(spark, name) for name in BRONZE_TABLES}


def create_silver_tables(spark) -> list:
    from common import config
    root = _abfss("SILVER_ROOT", config.SILVER_ROOT, "abfss://silver@<account>.dfs.core.windows.net")
    return _create(spark, "silver", SILVER_TABLES, root, "silver_root")


def create_gold_tables(spark) -> list:
    from common import config
    root = _abfss("GOLD_ROOT", config.GOLD_ROOT, "abfss://gold@<account>.dfs.core.windows.net")
    return _create(spark, "gold", GOLD_TABLES, root, "gold_root")


def create_ops_tables(spark) -> list:
    from common import config
    root = _abfss("OPS_ROOT", config.OPS_ROOT,
                  "abfss://gold@<account>.dfs.core.windows.net/ops (ops/ in the container gold, from GOLD_ROOT)")
    return _create(spark, "ops", OPS_TABLES, root, "ops_root")


def create_tables(spark) -> list:
    """Create every bronze, silver, gold and ops table if missing, in one job task (setup_tables) before the loads:
    tasks running in parallel would otherwise create the shared silver.load_log at the same time."""
    return create_bronze_tables(spark) + create_silver_tables(spark) + create_gold_tables(spark) + \
        create_ops_tables(spark)


def _run(spark, text: str) -> dict:
    """Run every statement of a SQL text; returns the summed row counts of their results (MERGE, INSERT)."""
    counts = {"num_inserted_rows": 0, "num_updated_rows": 0}
    for statement in statements(text):
        row = spark.sql(statement).first()
        result = row.asDict() if row is not None else {}
        for key in counts:
            counts[key] += int(result.get(key) or 0)
    return counts


def load_silver_table(spark, name: str) -> dict:
    """Load one silver table. Incremental: every new bronze batch, oldest first, each recorded in silver.load_log;
    returns {"batches": n}. Full load: rebuilds the table; returns {"rows": n}."""
    from common import config
    catalog = config.UC_CATALOG
    if name in SILVER_FULL:
        counts = _run(spark, render("silver/load", name, catalog=catalog))
        log.info("silver.%s: full load, %s rows", name, counts["num_inserted_rows"])
        return {"rows": counts["num_inserted_rows"]}
    if name not in SILVER_INCREMENTAL:
        raise ValueError(f"unknown silver table {name!r}, expected one of {SILVER_LOADED}")
    source = SILVER_INCREMENTAL[name]
    rows = spark.sql(render("silver", "new_batches", catalog=catalog, source=source, table=name)).collect()
    batches = [r["batch_id"] for r in rows]
    for batch_id in batches:
        if not BATCH_ID.fullmatch(batch_id):  # it goes inside a quoted SQL literal
            raise ValueError(f"unexpected batch_id {batch_id!r} in bronze.{source}")
        counts = _run(spark, render("silver/load", name, catalog=catalog, batch_id=batch_id))
        spark.sql(render("silver", "log_batch", catalog=catalog, table=name, batch_id=batch_id,
                         inserted=str(counts["num_inserted_rows"]), updated=str(counts["num_updated_rows"])))
        log.info("silver.%s: batch %s, %s rows inserted, %s updated", name, batch_id,
                 counts["num_inserted_rows"], counts["num_updated_rows"])
    return {"batches": len(batches)}


def load_silver_tables(spark) -> dict:
    """Load every silver table: the incremental ones first, then the full loads (they read silver.movie...).
    Returns {table: {"batches": n} or {"rows": n}}."""
    from common import config
    return {f"{config.UC_CATALOG}.silver.{name}": load_silver_table(spark, name) for name in SILVER_LOADED}


def _full_load(spark, layer: str, name: str, tables: list) -> dict:
    """Rebuild one table of a layer whose tables are all full loads. Returns {"rows": n}."""
    from common import config
    if name not in tables:
        raise ValueError(f"unknown {layer} table {name!r}, expected one of {tables}")
    counts = _run(spark, render(f"{layer}/load", name, catalog=config.UC_CATALOG))
    log.info("%s.%s: full load, %s rows", layer, name, counts["num_inserted_rows"])
    return {"rows": counts["num_inserted_rows"]}


def load_gold_table(spark, name: str) -> dict:
    """Rebuild one gold table from silver (full load). Returns {"rows": n}."""
    return _full_load(spark, "gold", name, GOLD_TABLES)


def load_gold_tables(spark) -> dict:
    """Rebuild every gold table. Returns {table: {"rows": n}}."""
    from common import config
    return {f"{config.UC_CATALOG}.gold.{name}": load_gold_table(spark, name) for name in GOLD_TABLES}


def load_ops_table(spark, name: str) -> dict:
    """Rebuild one ops table from bronze and silver.load_log (full load). Returns {"rows": n}."""
    return _full_load(spark, "ops", name, OPS_TABLES)


def load_ops_tables(spark) -> dict:
    """Rebuild every ops table. Returns {table: {"rows": n}}."""
    from common import config
    return {f"{config.UC_CATALOG}.ops.{name}": load_ops_table(spark, name) for name in OPS_TABLES}


def create_marts_views(spark) -> list:
    """Create or replace every view of the marts schema (pipeline.py marts). Views hold no data: they read gold and
    ops when queried, so they only need to be recreated when their SQL changes. Returns the views."""
    from common import config
    views = []
    for name in MARTS_VIEWS:
        for statement in statements(render("marts", name, catalog=config.UC_CATALOG)):
            spark.sql(statement)
        views.append(f"{config.UC_CATALOG}.marts.{name}")
        log.info("%s.marts.%s: view created or replaced", config.UC_CATALOG, name)
    return views
