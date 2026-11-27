"""Batch ingest of TMDB, one batch -> three files in <BRONZE_ROOT>/tmdb/<folder>/dt=<ds>/batch_id=<batch id>/:
  movies/movies.jsonl                    the movies of the movie lists and of trending/movie/day
  trending_movies/trending_movies.jsonl  the rank of today's trending movies, in TMDB's order
  tv_shows/tv_shows.jsonl                the popular TV shows (series)

No API key -> MissingCredentials (the task is skipped). HTTP errors on a genre list or no movie at all ->
SourceError, so the task fails and Databricks retries it. A failed page ends its endpoint with a warning; no
trending movie or no TV show still writes an empty file. Errors never include the request URL, which carries the
API key.

Endpoints: movie/popular and movie/top_rated (TMDB_PAGES pages each), the new movies of movie/now_playing (in
cinemas) and movie/upcoming (TMDB_NEW_PAGES pages each), trending/movie/day (TMDB_TRENDING_PAGES pages) and
tv/popular (TMDB_TV_PAGES pages), plus genre/movie/list and genre/tv/list for the genre names. Paging stops early at
the last page of an endpoint.

Rate limit: requests are sent one at a time on one connection, with a pause of PAUSE_S after each, so at most
4 requests/s: about 10x under TMDB's ceiling (around 40-50 requests/s per IP) and within its old limit of 40 per
10 s. A 429 is retried after its Retry-After (common/http.py). A run makes at most
2 + 2 x TMDB_PAGES + 2 x TMDB_NEW_PAGES + TMDB_TRENDING_PAGES + TMDB_TV_PAGES requests (58).
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
ENDPOINTS = ["movie/popular", "movie/top_rated", "movie/now_playing", "movie/upcoming", "trending/movie/day"]
NEW_MOVIES = {"movie/now_playing", "movie/upcoming"}  # paged with TMDB_NEW_PAGES
TRENDING = "trending/movie/day"  # paged with TMDB_TRENDING_PAGES; its movies are also ranked
TV_SHOWS = "tv/popular"  # paged with TMDB_TV_PAGES
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


def _pages(ep: str) -> int:
    if ep == TRENDING:
        return config.TMDB_TRENDING_PAGES
    return config.TMDB_NEW_PAGES if ep in NEW_MOVIES else config.TMDB_PAGES


def _genres(sess, auth: dict, path: str) -> dict:
    """{genre id: name} of a TMDB genre list (genre/movie/list, genre/tv/list)."""
    r = _get(sess, path, auth)
    if r.status_code != 200:
        raise SourceError("tmdb", f"GET {path} -> HTTP {r.status_code}", status=r.status_code)
    return {g["id"]: g["name"] for g in r.json().get("genres", [])}


def _results(sess, auth: dict, ep: str, pages: int, genres: dict, now: str) -> list:
    """The items of the first `pages` pages of an endpoint, in order, with their genre names, endpoint and ingest
    time. Stops at the endpoint's last page; a failed page is logged and ends the endpoint."""
    items = []
    for page in range(1, pages + 1):
        r = _get(sess, ep, {**auth, "page": page, "language": "en-US"})
        if r.status_code != 200:
            log.warning("TMDB %s page %s -> HTTP %s", ep, page, r.status_code)
            break
        payload = r.json()
        results = payload.get("results", [])
        if not results:
            break
        for m in results:
            m["genres"] = [genres[g] for g in m.get("genre_ids", []) if g in genres]
            m["_endpoint"] = ep
            m["_ingested_at"] = now
            items.append(m)
        if page >= payload.get("total_pages", pages):  # the last page of this endpoint
            break
    return items


def _write(ds: str, batch_id: str, folder: str, records: list) -> int:
    key = bronze_key("tmdb", folder, ds, batch_id, f"{folder}.jsonl")
    n = storage.put_jsonl(key, records)
    log.info("TMDB: wrote %s records to %s", n, storage.bronze_path(key))
    return n


def run(ds: str, batch_id: str) -> int:
    """Fetch and write one batch (the three files). Returns the number of movie records."""
    sess = session("MediaPlatformThesis/1.0", proxy=config.TMDB_PROXY)
    auth = _auth(sess)
    genres = _genres(sess, auth, "genre/movie/list")
    now = datetime.now(timezone.utc).isoformat()
    records, ranks = [], {}  # ranks: TMDB id -> rank in trending/movie/day, 1 = first
    for ep in ENDPOINTS:
        movies = _results(sess, auth, ep, _pages(ep), genres, now)
        records.extend(movies)
        if ep == TRENDING:
            for m in movies:  # a movie repeated on a later page keeps its rank
                if m.get("id") is not None:
                    ranks.setdefault(m["id"], len(ranks) + 1)
    if not records:
        raise SourceError("tmdb", f"no movies returned by {', '.join(ENDPOINTS)}")
    tv_genres = _genres(sess, auth, "genre/tv/list")
    shows = _results(sess, auth, TV_SHOWS, config.TMDB_TV_PAGES, tv_genres, now)
    # Every file is written, empty if need be: COPY INTO of a missing folder fails.
    n = _write(ds, batch_id, "movies", records)
    if not ranks:
        log.warning("TMDB: no movies from %s", TRENDING)
    _write(ds, batch_id, "trending_movies", [{"id": i, "rank": r, "_ingested_at": now} for i, r in ranks.items()])
    if not shows:
        log.warning("TMDB: no TV shows from %s", TV_SHOWS)
    _write(ds, batch_id, "tv_shows", shows)
    return n
