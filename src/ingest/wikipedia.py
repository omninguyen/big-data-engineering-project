"""Batch ingest: Wikipedia pageviews of the movies, TV shows and albums known to silver, in two steps.

1. Wikidata SPARQL maps the ids to Wikipedia articles: TMDB movie id (property P4947), TMDB TV series id (P4983)
   and MusicBrainz release group id (P436) -> the article about the same item on WIKI_PROJECT (en.wikipedia).
   Ids go WIKI_SPARQL_BATCH at a time.
   -> <BRONZE_ROOT>/wikidata/media_articles/dt=<ds>/batch_id=<batch id>/media_articles.jsonl
2. The Wikimedia pageviews API gives the daily views of every mapped article, by people only (agent=user), for the
   last WIKI_DAYS days up to the day before the batch day. One request covers all the days, so every run fetches the
   whole window again: a missed run or a late day is filled by the next one (silver keeps one value per article and
   day). Pageview days are UTC days, as Wikimedia counts them.
   -> <BRONZE_ROOT>/wikipedia/pageviews/dt=<ds>/batch_id=<batch id>/pageviews.jsonl

Wikimedia requires a User-Agent with a contact (secret mb-contact). Rate limits: one SPARQL query at a time with a
pause of SPARQL_PAUSE_S, pageviews at most 1 / PAUSE_S requests/s, far under Wikimedia's limits. A 429 or 5xx is
retried after a wait; a 404 means no views in the window. A 403 (refused User-Agent), a failed SPARQL query or no
article at all -> SourceError, so the task fails and Databricks retries it. Both files are written at the end only.
"""
import logging
import re
import time
from datetime import date, datetime, timedelta, timezone
from urllib.parse import quote, unquote

import requests

from common import config, storage
from common.errors import SourceError
from common.http import contact_user_agent, session
from ingest.batch import bronze_key

log = logging.getLogger(__name__)
SPARQL_PAUSE_S = 1.0  # after every SPARQL query
PAUSE_S = 0.1  # after every pageviews request: at most 10 requests/s
MAX_ATTEMPTS = 4
RETRY = {429, 500, 502, 503, 504}
PROPERTY = {"movie": "P4947", "tv": "P4983", "music": "P436"}  # Wikidata property holding the TMDB / MusicBrainz id
VALID_ID = {"movie": re.compile(r"[0-9]+"), "tv": re.compile(r"[0-9]+"),
            "music": re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")}


def media_ids() -> dict:
    """Ids of the current movies, TV shows and albums in silver (the task runs after their silver tasks)."""
    from pyspark.sql import SparkSession  # preinstalled on Databricks
    spark = SparkSession.builder.getOrCreate()
    catalog = config.UC_CATALOG
    movies = spark.sql(f"SELECT movie_id FROM {catalog}.silver.movie WHERE is_current ORDER BY movie_id").collect()
    albums = spark.sql(f"SELECT release_group_id FROM {catalog}.silver.release_group "
                       "WHERE is_current ORDER BY release_group_id").collect()
    shows = spark.sql(f"SELECT tv_id FROM {catalog}.silver.tv_show WHERE is_current ORDER BY tv_id").collect()
    return {"movie": [str(r["movie_id"]) for r in movies], "tv": [str(r["tv_id"]) for r in shows],
            "music": [r["release_group_id"] for r in albums]}


def sparql_query(media_type: str, ids: list) -> str:
    """The items holding one of `ids` in the property of `media_type`, and their article on WIKI_PROJECT."""
    values = " ".join(f'"{i}"' for i in ids)  # ids are checked against VALID_ID: nothing to escape
    return (f"SELECT ?id ?item ?article WHERE {{ VALUES ?id {{ {values} }} "
            f"?item wdt:{PROPERTY[media_type]} ?id . "
            f"?article schema:about ?item ; schema:isPartOf <https://{config.WIKI_PROJECT}.org/> . }}")


def _sparql(sess, query: str) -> list:
    """Bindings of a SPARQL query, retrying a 429, a 5xx or a network error."""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        wait = SPARQL_PAUSE_S * 2 ** attempt
        try:
            r = sess.post(config.WIKI_SPARQL_URL, data={"query": query},
                          headers={"Accept": "application/sparql-results+json"}, timeout=90)
        except requests.RequestException as e:
            problem = type(e).__name__
        else:
            if r.status_code == 200:
                time.sleep(SPARQL_PAUSE_S)
                return r.json().get("results", {}).get("bindings", [])
            if r.status_code not in RETRY:
                raise SourceError("wikidata", f"SPARQL query -> HTTP {r.status_code}", status=r.status_code)
            problem = f"HTTP {r.status_code}"
            try:
                wait = max(wait, float(r.headers.get("Retry-After", "0")))
            except ValueError:
                pass
        if attempt < MAX_ATTEMPTS:
            log.warning("Wikidata: %s, attempt %s of %s, waiting %.0f s", problem, attempt, MAX_ATTEMPTS, wait)
            time.sleep(wait)
    raise SourceError("wikidata", f"SPARQL query failed {MAX_ATTEMPTS} times: {problem}")


def articles(sess, ids: dict, now: str) -> list:
    """One record per id and article: media_type, source_id, wikidata_id, article (title with underscores), url."""
    records = []
    for media_type, all_ids in ids.items():
        valid = [i for i in all_ids if VALID_ID[media_type].fullmatch(i or "")]
        for start in range(0, len(valid), config.WIKI_SPARQL_BATCH):
            for b in _sparql(sess, sparql_query(media_type, valid[start:start + config.WIKI_SPARQL_BATCH])):
                url = b["article"]["value"]
                records.append({
                    "media_type": media_type,
                    "source_id": b["id"]["value"],
                    "wikidata_id": b["item"]["value"].rsplit("/", 1)[-1],
                    "article": unquote(url.split("/wiki/", 1)[-1]),
                    "url": url,
                    "_ingested_at": now,
                })
    return records


def window(ds: str) -> tuple:
    """First and last day of the pageviews to fetch: the WIKI_DAYS days up to the day before the batch day."""
    end = date.fromisoformat(ds) - timedelta(days=1)
    return end - timedelta(days=config.WIKI_DAYS - 1), end


def pageviews_url(article: str, first: date, last: date) -> str:
    title = quote(article.replace(" ", "_"), safe="")  # a "/" in a title must be encoded too
    return (f"{config.WIKI_PAGEVIEWS_URL}/{config.WIKI_PROJECT}/all-access/user/{title}/daily/"
            f"{first:%Y%m%d}/{last:%Y%m%d}")


def pageviews(sess, titles: list, first: date, last: date, now: str) -> tuple:
    """Daily views of every article: (records, articles without views, failed requests). The session retries a 429
    or 5xx (common/http.py); a 403 means Wikimedia refuses the User-Agent and fails the run."""
    records, empty, failed = [], 0, 0
    for article in titles:
        try:
            r = sess.get(pageviews_url(article, first, last), timeout=30)
        except requests.RequestException as e:
            log.warning("Wikipedia pageviews of %s failed: %s", article, type(e).__name__)
            failed += 1
            continue
        finally:
            time.sleep(PAUSE_S)
        if r.status_code == 404:  # no views in the window
            empty += 1
        elif r.status_code == 403:
            raise SourceError("wikipedia", "pageviews -> HTTP 403: check the User-Agent contact (secret mb-contact)",
                              status=403)
        elif r.status_code != 200:
            log.warning("Wikipedia pageviews of %s -> HTTP %s", article, r.status_code)
            failed += 1
        else:
            for item in r.json().get("items", []):
                records.append({**item, "_ingested_at": now})
    return records, empty, failed


def run(ds: str, batch_id: str) -> int:
    sess = session(contact_user_agent())
    now = datetime.now(timezone.utc).isoformat()
    mapped = articles(sess, media_ids(), now)
    if not mapped:
        raise SourceError("wikidata", "no movie or album of silver has a Wikipedia article")
    titles = sorted({m["article"] for m in mapped})
    first, last = window(ds)
    views, empty, failed = pageviews(sess, titles, first, last, now)
    if not views:
        raise SourceError("wikipedia", f"no pageviews for any of {len(titles)} articles")
    key = bronze_key("wikidata", "media_articles", ds, batch_id, "media_articles.jsonl")
    n_articles = storage.put_jsonl(key, mapped)
    key = bronze_key("wikipedia", "pageviews", ds, batch_id, "pageviews.jsonl")
    n = storage.put_jsonl(key, views)
    log.info("Wikipedia: %s ids mapped to %s articles, %s daily views from %s to %s (%s articles without views, "
             "%s failed requests), wrote %s + %s records", n_articles, len(titles), n, first, last, empty, failed,
             n_articles, n)
    return n
