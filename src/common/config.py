"""Central configuration, read from environment variables.

On Databricks serverless jobs compute there are no cluster environment variables: src/pipeline.py sets them from
`--set KEY=VALUE` task parameters before this module is imported. Secrets come from the Databricks secret scope
DATABRICKS_SECRET_SCOPE, never from the job definition.
"""
import os
import re


def _get(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


ON_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ  # set by every Databricks runtime
DATABRICKS_SECRET_SCOPE = _get("DATABRICKS_SECRET_SCOPE", "media-platform")


def _secret(name: str, key: str, default: str = "") -> str:
    """Environment variable first; on Databricks, fall back to the secret scope. A missing scope or key gives
    `default` instead of an error, so optional sources (TMDB) can be skipped cleanly."""
    value = _get(name)
    if value or not ON_DATABRICKS:
        return value or default
    try:
        from databricks.sdk.runtime import dbutils  # preinstalled on Databricks runtimes
        return dbutils.secrets.get(DATABRICKS_SECRET_SCOPE, key).strip()
    except Exception:
        return default


# ================= Unity Catalog =================
UC_CATALOG = _get("UC_CATALOG", "big_data_engineering")
if not re.fullmatch(r"[A-Za-z0-9_]+", UC_CATALOG):
    raise ValueError(f"UC_CATALOG may only contain letters, digits and underscores, got {UC_CATALOG!r}")
# Raw files: the folder landing/ of the ADLS container bronze (a Unity Catalog external location), e.g.
# abfss://bronze@<account>.dfs.core.windows.net/landing. The Delta tables go next to it, in tables/.
# Set by the job with --set BRONZE_ROOT=... A local folder also works, outside Databricks.
BRONZE_ROOT = _get("BRONZE_ROOT").rstrip("/")
# Delta tables of the bronze layer (pipeline.py bronze): tables/ next to landing/, unless BRONZE_TABLES_ROOT is set.
BRONZE_TABLES_ROOT = _get("BRONZE_TABLES_ROOT").rstrip("/") or (
    BRONZE_ROOT[:-len("landing")] + "tables" if BRONZE_ROOT.endswith("/landing") else "")
# Delta tables of the silver layer: the ADLS container silver, e.g. abfss://silver@<account>.dfs.core.windows.net
# (a Unity Catalog external location). Set by the job with --set SILVER_ROOT=...
SILVER_ROOT = _get("SILVER_ROOT").rstrip("/")
# Delta tables of the gold layer: the ADLS container gold, e.g. abfss://gold@<account>.dfs.core.windows.net
# (a Unity Catalog external location). Set by the job with --set GOLD_ROOT=...
GOLD_ROOT = _get("GOLD_ROOT").rstrip("/")
# Delta tables of the ops schema (pipeline metrics, data quality): ops/ in the container gold, unless OPS_ROOT is set.
OPS_ROOT = _get("OPS_ROOT").rstrip("/") or (f"{GOLD_ROOT}/ops" if GOLD_ROOT else "")

# ================= sources =================
TMDB_API_KEY = _secret("TMDB_API_KEY", "tmdb-api-key")
TMDB_PROXY = _get("TMDB_PROXY")  # optional HTTP proxy, only for TMDB calls
TMDB_BASE_URL = _get("TMDB_BASE_URL", "https://api.themoviedb.org/3").rstrip("/")
MOVIE_SOURCE = _get("MOVIE_SOURCE", "tmdb").lower()  # tmdb | file (exported by the browser) | sample (demo set)
if MOVIE_SOURCE not in ("tmdb", "file", "sample"):
    raise ValueError(f"MOVIE_SOURCE must be tmdb, file or sample, got {MOVIE_SOURCE!r}")
MOVIE_FILE = _get("MOVIE_FILE") or (f"{BRONZE_ROOT}/manual/tmdb_movies.json" if BRONZE_ROOT else "")
TMDB_PAGES = int(_get("TMDB_PAGES", "20"))  # pages of movie/popular and movie/top_rated (20 movies each)
TMDB_NEW_PAGES = int(_get("TMDB_NEW_PAGES", "5"))  # pages of movie/now_playing and movie/upcoming: new movies
MB_CONTACT = _get("MB_CONTACT", "you@example.com")  # MusicBrainz requires a contact in the User-Agent
MB_GENRES = [g.strip() for g in _get(
    "MB_GENRES", "rock,pop,jazz,electronic,hip hop,classical,metal,folk,r&b,indie,country,soul"
).split(",") if g.strip()]
MB_PAGES_PER_GENRE = int(_get("MB_PAGES_PER_GENRE", "2"))
MB_NEW_DAYS = int(_get("MB_NEW_DAYS", "30"))  # also search the albums of each genre released in the last N days
LB_BASE_URL = _get("LB_BASE_URL", "https://api.listenbrainz.org").rstrip("/")  # ListenBrainz, no token needed
LB_BATCH_SIZE = int(_get("LB_BATCH_SIZE", "100"))  # albums per popularity request (the documented limit may be higher)
