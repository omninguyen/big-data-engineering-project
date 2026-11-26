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
# Raw files land in the volume <catalog>.bronze.landing
UC_CATALOG = _get("UC_CATALOG", "big_data_engineering")
if not re.fullmatch(r"[A-Za-z0-9_]+", UC_CATALOG):
    raise ValueError(f"UC_CATALOG may only contain letters, digits and underscores, got {UC_CATALOG!r}")
UC_LANDING_ROOT = (_get("UC_LANDING_ROOT") or f"/Volumes/{UC_CATALOG}/bronze/landing").rstrip("/")

# ================= sources =================
TMDB_API_KEY = _secret("TMDB_API_KEY", "tmdb-api-key")
TMDB_PROXY = _get("TMDB_PROXY")  # optional HTTP proxy, only for TMDB calls
TMDB_BASE_URL = _get("TMDB_BASE_URL", "https://api.themoviedb.org/3").rstrip("/")
MOVIE_SOURCE = _get("MOVIE_SOURCE", "tmdb").lower()  # tmdb | file (exported by the browser) | sample (demo set)
if MOVIE_SOURCE not in ("tmdb", "file", "sample"):
    raise ValueError(f"MOVIE_SOURCE must be tmdb, file or sample, got {MOVIE_SOURCE!r}")
MOVIE_FILE = _get("MOVIE_FILE") or f"{UC_LANDING_ROOT}/manual/tmdb_movies.json"
TMDB_PAGES = int(_get("TMDB_PAGES", "20"))
MB_CONTACT = _get("MB_CONTACT", "you@example.com")  # MusicBrainz requires a contact in the User-Agent
MB_GENRES = [g.strip() for g in _get(
    "MB_GENRES", "rock,pop,jazz,electronic,hip hop,classical,metal,folk,r&b,indie,country,soul"
).split(",") if g.strip()]
MB_PAGES_PER_GENRE = int(_get("MB_PAGES_PER_GENRE", "2"))
