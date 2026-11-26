"""Batch ingest: ListenBrainz popularity of the albums in silver.release_group ->
<BRONZE_ROOT>/listenbrainz/release_group_popularity/dt=<ds>/batch_id=<batch id>/release_group_popularity.jsonl

POST /1/popularity/release-group takes up to LB_BATCH_SIZE release group MBIDs and returns, for each album,
total_listen_count and total_user_count: all-time totals, null when ListenBrainz has no data for the album.
Every album known to silver is looked up, so each one gets a daily series, even on days MusicBrainz search does not
return it. No token: it would only raise the rate limit, and a run makes a few dozen requests.

Rate limit: ListenBrainz reports it in the headers X-RateLimit-Remaining and X-RateLimit-Reset-In (seconds). The run
pauses PAUSE_S after each request, waits for the reset when the window is used up, and retries a 429, a 5xx or a
network error after that wait (MAX_ATTEMPTS per request). Any other HTTP error, or no album at all -> SourceError,
so the task fails and Databricks retries it. The file is written once, at the end: a failed run leaves nothing.
"""
import logging
import re
import time
from datetime import datetime, timezone

import requests

from common import config, storage
from common.errors import SourceError
from common.http import session
from ingest.batch import bronze_key

log = logging.getLogger(__name__)
PAUSE_S = 0.5  # after every request: at most 2 requests/s
MAX_ATTEMPTS = 4
RETRY = {429, 500, 502, 503, 504}
MBID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


def album_ids() -> list:
    """Release group MBIDs of the current albums in silver.release_group (the task runs after silver_release_group)."""
    from pyspark.sql import SparkSession  # preinstalled on Databricks
    spark = SparkSession.builder.getOrCreate()
    rows = spark.sql(f"SELECT release_group_id FROM {config.UC_CATALOG}.silver.release_group "
                     "WHERE is_current ORDER BY release_group_id").collect()
    return [r["release_group_id"] for r in rows]


def _wait_s(response) -> float:
    """Seconds to wait before the next request: until the reset when the rate limit window is used up or the
    request was refused (429), else PAUSE_S."""
    try:
        remaining = int(response.headers.get("X-RateLimit-Remaining", "1"))
        reset_in = float(response.headers.get("X-RateLimit-Reset-In", "0"))
    except ValueError:  # a header that is not a number: no information
        remaining, reset_in = 1, 0.0
    if response.status_code == 429 or remaining <= 0:
        return max(reset_in, PAUSE_S) + 1  # one more second against clock skew
    return PAUSE_S


def _post(sess, mbids: list) -> list:
    """The popularity of up to LB_BATCH_SIZE albums, retrying a 429, a 5xx or a network error."""
    url = f"{config.LB_BASE_URL}/1/popularity/release-group"
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            r = sess.post(url, json={"release_group_mbids": mbids}, timeout=30)
        except requests.RequestException as e:
            problem, wait = type(e).__name__, PAUSE_S * 2 ** attempt
        else:
            wait = _wait_s(r)
            if r.status_code == 200:
                time.sleep(wait)
                result = r.json()
                if not isinstance(result, list):
                    raise SourceError("listenbrainz", f"unexpected answer: {type(result).__name__}, not a list")
                return result
            if r.status_code not in RETRY:
                raise SourceError("listenbrainz", f"POST popularity/release-group -> HTTP {r.status_code}",
                                  status=r.status_code)
            problem, wait = f"HTTP {r.status_code}", max(wait, PAUSE_S * 2 ** attempt)  # 429: the reset; 5xx: back off
        if attempt < MAX_ATTEMPTS:
            log.warning("ListenBrainz: %s, attempt %s of %s, waiting %.1f s", problem, attempt, MAX_ATTEMPTS, wait)
            time.sleep(wait)
    raise SourceError("listenbrainz", f"POST popularity/release-group failed {MAX_ATTEMPTS} times: {problem}")


def run(ds: str, batch_id: str) -> int:
    ids = [i for i in album_ids() if MBID.fullmatch(i or "")]  # anything else would make the request fail (400)
    if not ids:
        raise SourceError("listenbrainz", "no album in silver.release_group to look up")
    sess = session(f"MediaPlatformThesis/1.0 ( {config.MB_CONTACT} )")
    now = datetime.now(timezone.utc).isoformat()
    records = []
    for start in range(0, len(ids), config.LB_BATCH_SIZE):
        for item in _post(sess, ids[start:start + config.LB_BATCH_SIZE]):
            records.append({
                "release_group_mbid": item.get("release_group_mbid"),
                "total_listen_count": item.get("total_listen_count"),
                "total_user_count": item.get("total_user_count"),
                "_ingested_at": now,
            })
    known = sum(1 for r in records if r["total_listen_count"] is not None)
    key = bronze_key("listenbrainz", "release_group_popularity", ds, batch_id, "release_group_popularity.jsonl")
    n = storage.put_jsonl(key, records)
    log.info("ListenBrainz: %s albums looked up, %s with listens, wrote %s records to %s",
             len(ids), known, n, storage.bronze_path(key))
    return n
