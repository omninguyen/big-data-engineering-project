"""Batch ingest: MusicBrainz release-groups (albums) per genre tag -> <BRONZE_ROOT>/musicbrainz/...
For each genre (MB_GENRES): the albums that match it best (MB_PAGES_PER_GENRE pages of 100), and the albums released
in the last MB_NEW_DAYS days (one page), so new music keeps coming in.
Rate limit: 1 request/second (we sleep 1.1s). A contact in the User-Agent is required (MB_CONTACT).
A failed search is logged and skipped. No album at all (network down, every request rejected) -> SourceError, so the
task fails and is retried."""
import logging
import time
from datetime import date, datetime, timedelta, timezone

import requests

from common import config, storage
from common.errors import SourceError
from common.http import session
from ingest.batch import bronze_key

log = logging.getLogger(__name__)
URL = "https://musicbrainz.org/ws/2/release-group"
PAUSE_S = 1.1  # after every request: under 1 request/s


def _search(sess, query: str, offset: int, label: str) -> list:
    """One page of release groups (100), or [] when the request fails (logged)."""
    try:
        r = sess.get(URL, params={"query": query, "fmt": "json", "limit": 100, "offset": offset}, timeout=30)
    except requests.RequestException as e:
        log.warning("MusicBrainz %s failed: %s", label, type(e).__name__)
        return []
    finally:
        time.sleep(PAUSE_S)
    if r.status_code != 200:
        log.warning("MusicBrainz %s -> HTTP %s", label, r.status_code)
        return []
    return r.json().get("release-groups", [])


def run(ds: str, batch_id: str) -> int:
    sess = session(f"MediaPlatformThesis/1.0 ( {config.MB_CONTACT} )")
    now = datetime.now(timezone.utc).isoformat()
    since = (date.fromisoformat(ds) - timedelta(days=config.MB_NEW_DAYS)).isoformat()
    records = []

    def keep(groups, genre):
        for g in groups:
            g["_query_genre"] = genre
            g["_ingested_at"] = now
            records.append(g)

    for genre in config.MB_GENRES:
        albums = f'tag:"{genre}" AND primarytype:album'
        for page in range(config.MB_PAGES_PER_GENRE):  # the albums that match the genre best
            groups = _search(sess, albums, page * 100, f"{genre} page {page}")
            if not groups:
                break
            keep(groups, genre)
        # new albums: first released in the last MB_NEW_DAYS days, up to the batch day
        keep(_search(sess, f"{albums} AND firstreleasedate:[{since} TO {ds}]", 0, f"{genre} released since {since}"),
             genre)
    if not records:
        raise SourceError("musicbrainz", f"no release groups returned for genres {config.MB_GENRES}")
    key = bronze_key("musicbrainz", "release_groups", ds, batch_id, "release_groups.jsonl")
    n = storage.put_jsonl(key, records)
    log.info("MusicBrainz: wrote %s records to %s", n, storage.bronze_path(key))
    return n
