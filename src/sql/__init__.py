"""SQL of the Unity Catalog tables, one statement per file, run with spark.sql (pipeline.py tables).

src/sql/bronze/<table>.sql       creates <catalog>.bronze.<table>, an external Delta table in <tables root>/<table>
src/sql/bronze/load/<table>.sql  loads the new files of landing/ into it with COPY INTO
${catalog}, ${tables_root} and ${landing_root} are filled in from common/config.py (UC_CATALOG, BRONZE_TABLES_ROOT,
BRONZE_ROOT), so the files hold no workspace or storage details.
"""
import logging
import os
import re
from string import Template

log = logging.getLogger(__name__)
HERE = os.path.dirname(os.path.abspath(__file__))
BRONZE_TABLES = ["tmdb_movies", "musicbrainz_release_groups"]


def render(layer: str, name: str, **params: str) -> str:
    """The statement in src/sql/<layer>/<name>.sql. A placeholder without a value raises KeyError."""
    with open(os.path.join(HERE, layer, f"{name}.sql"), encoding="utf-8") as f:
        return Template(f.read()).substitute(params)


def _abfss(setting: str, value: str) -> str:
    """The value, if it is a plain abfss:// path (it goes inside a quoted SQL literal)."""
    if not re.fullmatch(r"abfss://[\w.@/=-]+", value):
        raise ValueError(f"{setting} must be an abfss:// path, got {value!r}. Set BRONZE_ROOT to "
                         "abfss://bronze@<account>.dfs.core.windows.net/landing: the tables go next to it, in tables/")
    return value


def create_bronze_tables(spark) -> list:
    """Create the bronze tables if missing. A table of the same name somewhere else (e.g. a managed table) fails:
    CREATE TABLE IF NOT EXISTS would silently keep it."""
    from common import config
    root = _abfss("BRONZE_TABLES_ROOT", config.BRONZE_TABLES_ROOT)
    tables = []
    for name in BRONZE_TABLES:
        table, location = f"{config.UC_CATALOG}.bronze.{name}", f"{root}/{name}"
        spark.sql(render("bronze", name, catalog=config.UC_CATALOG, tables_root=root))
        actual = spark.sql(f"DESCRIBE DETAIL {table}").first()["location"].rstrip("/")
        if actual != location:
            raise RuntimeError(f"{table} already exists in {actual}, not in {location}: "
                               "drop it (it may be a managed table) and run again")
        log.info("%s: external Delta table in %s", table, location)
        tables.append(table)
    return tables


def load_bronze_tables(spark) -> dict:
    """COPY INTO every bronze table from landing/. Returns {table: COPY INTO result, e.g. num_inserted_rows}."""
    from common import config
    landing = _abfss("BRONZE_ROOT", config.BRONZE_ROOT)
    loaded = {}
    for name in BRONZE_TABLES:
        table = f"{config.UC_CATALOG}.bronze.{name}"
        row = spark.sql(render("bronze/load", name, catalog=config.UC_CATALOG, landing_root=landing)).first()
        loaded[table] = row.asDict() if row is not None else {}
        log.info("%s: COPY INTO %s", table, loaded[table])
    return loaded
