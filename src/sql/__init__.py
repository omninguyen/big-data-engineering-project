"""SQL of the Unity Catalog tables, run with spark.sql (pipeline.py bronze, pipeline.py silver).

src/sql/bronze/<table>.sql       creates <catalog>.bronze.<table>, an external Delta table in <tables root>/<table>
src/sql/bronze/load/<table>.sql  loads the new files of landing/ into it with COPY INTO
src/sql/silver/<table>.sql       creates <catalog>.silver.<table>, an external Delta table in <silver root>/<table>
src/sql/silver/load/<table>.sql  loads one bronze batch into it; run once per new batch, oldest first, and recorded
                                 in silver.load_log (the watermark: a recorded batch is not loaded again)
${...} placeholders are filled in from common/config.py (UC_CATALOG, BRONZE_TABLES_ROOT, BRONZE_ROOT, SILVER_ROOT),
so the files hold no workspace or storage details. A file may hold several statements, each ending with ';' at the
end of a line.
"""
import logging
import os
import re
from string import Template

log = logging.getLogger(__name__)
HERE = os.path.dirname(os.path.abspath(__file__))
BRONZE_TABLES = ["tmdb_movies", "musicbrainz_release_groups"]
SILVER_TABLES = ["load_log", "movie", "movie_metric_daily"]
SILVER_INCREMENTAL = {"movie": "tmdb_movies", "movie_metric_daily": "tmdb_movies"}  # silver table: its bronze table
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


def load_bronze_tables(spark) -> dict:
    """COPY INTO every bronze table from landing/. Returns {table: COPY INTO result, e.g. num_inserted_rows}."""
    from common import config
    landing = _abfss("BRONZE_ROOT", config.BRONZE_ROOT, "abfss://bronze@<account>.dfs.core.windows.net/landing")
    loaded = {}
    for name in BRONZE_TABLES:
        table = f"{config.UC_CATALOG}.bronze.{name}"
        row = spark.sql(render("bronze/load", name, catalog=config.UC_CATALOG, landing_root=landing)).first()
        loaded[table] = row.asDict() if row is not None else {}
        log.info("%s: COPY INTO %s", table, loaded[table])
    return loaded


def create_silver_tables(spark) -> list:
    from common import config
    root = _abfss("SILVER_ROOT", config.SILVER_ROOT, "abfss://silver@<account>.dfs.core.windows.net")
    return _create(spark, "silver", SILVER_TABLES, root, "silver_root")


def _run(spark, text: str) -> dict:
    """Run every statement of a SQL text; returns the summed row counts of their results (MERGE, INSERT)."""
    counts = {"num_inserted_rows": 0, "num_updated_rows": 0}
    for statement in statements(text):
        row = spark.sql(statement).first()
        result = row.asDict() if row is not None else {}
        for key in counts:
            counts[key] += int(result.get(key) or 0)
    return counts


def load_silver_tables(spark) -> dict:
    """Load every new bronze batch into each incremental silver table, oldest first, and record it in
    silver.load_log. Returns {table: number of batches loaded}."""
    from common import config
    catalog, loaded = config.UC_CATALOG, {}
    for name, source in SILVER_INCREMENTAL.items():
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
        loaded[f"{catalog}.silver.{name}"] = len(batches)
    return loaded
