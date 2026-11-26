"""TMDB movies from a file saved by the browser (MOVIE_SOURCE=file).

Use when the network blocks Python/curl (TCP+TLS) but a browser still reaches TMDB (e.g. via HTTP/3).
The file is produced by docs/tmdb_browser_export.js (docs/HUONG_DAN.md) and uploaded to MOVIE_FILE,
by default <landing volume>/manual/tmdb_movies.json. Same bronze layout as ingest/tmdb.py.
"""
import json
import logging
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger(__name__)


def load(path: str, now: Optional[str] = None) -> list:
    """Read the browser file; returns bronze-ready records (deduplicated by TMDB id)."""
    try:
        with open(path, encoding="utf-8") as f:
            doc = json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"{path} not found - export it with docs/tmdb_browser_export.js and upload it, e.g. "
                                f"databricks fs cp tmdb_movies.json dbfs:{path}") from None
    if isinstance(doc, list):
        results, raw_genres = doc, {}
    else:
        results, raw_genres = doc.get("results", []), doc.get("genres", {})
    if isinstance(raw_genres, list):  # [{"id": 28, "name": "Action"}, ...]
        raw_genres = {g["id"]: g["name"] for g in raw_genres}
    genres = {int(k): v for k, v in raw_genres.items()}
    now = now or datetime.now(timezone.utc).isoformat()
    seen, out = set(), []
    for m in results:
        mid = m.get("id")
        if mid is None or not m.get("title") or mid in seen:
            continue
        seen.add(mid)
        if not m.get("genres"):
            m["genres"] = [genres[g] for g in m.get("genre_ids", []) if g in genres]
        m.setdefault("_endpoint", "file")
        m["_ingested_at"] = now
        out.append(m)
    if not out:
        raise ValueError(f"no movies found in {path}")
    return out


def run(ds: str) -> int:
    from common import config, storage  # imported here so load() stays a pure function
    records = load(config.MOVIE_FILE)
    key = f"bronze/tmdb/movies/dt={ds}/movies.jsonl"
    n = storage.put_jsonl(key, records)
    log.info("MOVIE_SOURCE=file: wrote %s movies from %s to %s", n, config.MOVIE_FILE, storage.volume_path(key))
    return n
