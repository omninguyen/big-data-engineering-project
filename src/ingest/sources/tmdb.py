"""TMDB: movies (movie/popular, movie/top_rated) and genres (genre/movie/list; the ids are mapped in transform).

No API key (secret tmdb-api-key) -> MissingCredentials, batch SKIPPED. The genre list failing, or no movie at all
-> SourceError, batch FAILED. A movie page that fails ends that endpoint and is counted, batch PARTIAL.
Errors never include the request URL, which carries the API key.
"""
import time

import requests

from common import config
from common.errors import MissingCredentials, SourceError
from common.http import session
from ingest.batch import Page

USER_AGENT = "MediaPlatformThesis/1.0"
ENDPOINTS = ["movie/popular", "movie/top_rated"]
LANGUAGE = "en-US"


def params() -> dict:
    return {"movie_source": "tmdb", "endpoints": ENDPOINTS, "pages": config.TMDB_PAGES, "language": LANGUAGE}


def _auth(sess) -> dict:
    key = config.TMDB_API_KEY
    if not key:
        raise MissingCredentials("TMDB_API_KEY is empty (secret tmdb-api-key) - skipping TMDB ingest")
    if len(key) > 40:  # v4 read access token (JWT)
        sess.headers["Authorization"] = f"Bearer {key}"
        return {}
    return {"api_key": key}


def _get(sess, path: str, query: dict):
    try:
        return sess.get(f"{config.TMDB_BASE_URL}/{path}", params=query, timeout=30)
    except requests.RequestException as e:  # the message would contain the URL with api_key
        raise SourceError("tmdb", f"GET {path} failed: {type(e).__name__}") from None


def extract(batch):
    sess = session(USER_AGENT, proxy=config.TMDB_PROXY, on_response=batch.http.on_response)
    auth = _auth(sess)
    r = _get(sess, "genre/movie/list", {**auth, "language": LANGUAGE})
    if r.status_code != 200:
        raise SourceError("tmdb", f"GET genre/movie/list -> HTTP {r.status_code}", status=r.status_code)
    yield Page("genres", r.json().get("genres", []), {"endpoint": "genre/movie/list"})

    movies = 0
    for endpoint in ENDPOINTS:
        for page in range(1, config.TMDB_PAGES + 1):
            request = {"endpoint": endpoint, "page": page}
            try:
                r = _get(sess, endpoint, {**auth, "page": page, "language": LANGUAGE})
            except SourceError as e:
                batch.fail_page(request, str(e))
                break
            if r.status_code != 200:
                batch.fail_page(request, f"HTTP {r.status_code}")
                break
            results = r.json().get("results", [])
            if not results:
                break
            movies += len(results)
            yield Page("movies", results, request)
            time.sleep(0.05)
    if not movies:
        raise SourceError("tmdb", f"no movies returned by {' and '.join(ENDPOINTS)}")
