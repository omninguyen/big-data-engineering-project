"""Batch ingest: TMDB movies -> <BRONZE_ROOT>/tmdb/movies/dt=<ds>/batch_id=<batch id>/movies.jsonl

No API key -> MissingCredentials (the task is skipped). HTTP errors or an empty result -> SourceError, so the task
fails and Databricks retries it. Errors never include the request URL, which carries the API key.

Rate limit: requests are sent one at a time on one connection, with a pause of PAUSE_S after each, so at most
4 requests/s: about 10x under TMDB's ceiling (around 40-50 requests/s per IP) and within its old limit of 40 per
10 s. A 429 is retried after its Retry-After (common/http.py). A run makes 1 + 2 x TMDB_PAGES requests (41).
"""
import logging
import time
from datetime import datetime, timezone

import requests

from common import config, storage
from common.errors import MissingCredentials, SourceError
from common.http import session
from ingest.batch import bronze_key

log = logging.getLogger(__name__)
ENDPOINTS = ["movie/popular", "movie/top_rated"]
PAUSE_S = 0.25  # after every request: at most 4 requests/s


def _auth(sess):
    key = config.TMDB_API_KEY
    if not key:
        raise MissingCredentials("TMDB_API_KEY is empty (secret tmdb-api-key) - skipping TMDB ingest")
    params = {}
    if len(key) > 40:  # v4 read access token (JWT)
        sess.headers["Authorization"] = f"Bearer {key}"
    else:
        params["api_key"] = key
    return params


def _get(sess, path: str, params: dict):
    try:
        return sess.get(f"{config.TMDB_BASE_URL}/{path}", params=params, timeout=30)
    except requests.RequestException as e:  # the message would contain the URL with api_key
        raise SourceError("tmdb", f"GET {path} failed: {type(e).__name__}") from None
    finally:
        time.sleep(PAUSE_S)  # also after a failed request


def run(ds: str, batch_id: str) -> int:
    sess = session("MediaPlatformThesis/1.0", proxy=config.TMDB_PROXY)
    auth = _auth(sess)
    r = _get(sess, "genre/movie/list", auth)
    if r.status_code != 200:
        raise SourceError("tmdb", f"GET genre/movie/list -> HTTP {r.status_code}", status=r.status_code)
    genres = {g["id"]: g["name"] for g in r.json().get("genres", [])}
    now = datetime.now(timezone.utc).isoformat()
    records = []
    for ep in ENDPOINTS:
        for page in range(1, config.TMDB_PAGES + 1):
            r = _get(sess, ep, {**auth, "page": page, "language": "en-US"})
            if r.status_code != 200:
                log.warning("TMDB %s page %s -> HTTP %s", ep, page, r.status_code)
                break
            results = r.json().get("results", [])
            if not results:
                break
            for m in results:
                m["genres"] = [genres[g] for g in m.get("genre_ids", []) if g in genres]
                m["_endpoint"] = ep
                m["_ingested_at"] = now
                records.append(m)
    if not records:
        raise SourceError("tmdb", "no movies returned by movie/popular and movie/top_rated")
    key = bronze_key("tmdb", "movies", ds, batch_id, "movies.jsonl")
    n = storage.put_jsonl(key, records)
    log.info("TMDB: wrote %s records to %s", n, storage.bronze_path(key))
    return n
