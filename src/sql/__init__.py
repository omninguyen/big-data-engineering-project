"""SQL of the Unity Catalog tables, one statement per file, run with spark.sql (pipeline.py tables).

src/sql/bronze/<table>.sql creates <catalog>.bronze.<table>, an external Delta table in <tables root>/<table>.
${catalog} and ${tables_root} are filled in from common/config.py (UC_CATALOG, BRONZE_TABLES_ROOT), so the files
hold no workspace or storage details.
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


def create_bronze_tables(spark) -> list:
    """Create the bronze tables if missing. A table of the same name somewhere else (e.g. a managed table) fails:
    CREATE TABLE IF NOT EXISTS would silently keep it."""
    from common import config
    root = config.BRONZE_TABLES_ROOT
    if not re.fullmatch(r"abfss://[\w.@/=-]+", root):
        raise ValueError(f"BRONZE_TABLES_ROOT must be an abfss:// path, got {root!r}. Set BRONZE_ROOT to "
                         "abfss://bronze@<account>.dfs.core.windows.net/landing: the tables go next to it, in tables/")
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
