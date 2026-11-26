"""Batch ingest: MusicBrainz release-groups (albums) per genre tag -> <landing volume>/musicbrainz/...
Rate limit: 1 request/second (we sleep 1.1s). A contact in the User-Agent is required (MB_CONTACT).
No album at all (network down, every request rejected) -> SourceError, so the task fails and is retried."""
import logging
import time
from datetime import datetime, timezone

import requests

from common import config, storage
from common.errors import SourceError
from common.http import session
from ingest.batch import bronze_key

log = logging.getLogger(__name__)
URL = "https://musicbrainz.org/ws/2/release-group"


def run(ds: str, batch_id: str) -> int:
    sess = session(f"MediaPlatformThesis/1.0 ( {config.MB_CONTACT} )")
    now = datetime.now(timezone.utc).isoformat()
    records = []
    for genre in config.MB_GENRES:
        for page in range(config.MB_PAGES_PER_GENRE):
            params = {
                "query": f'tag:"{genre}" AND primarytype:album',
                "fmt": "json", "limit": 100, "offset": page * 100,
            }
            try:
                r = sess.get(URL, params=params, timeout=30)
            except requests.RequestException as e:
                log.warning("MusicBrainz %s page %s failed: %s", genre, page, type(e).__name__)
                break
            finally:
                time.sleep(1.1)
            if r.status_code != 200:
                log.warning("MusicBrainz %s page %s -> HTTP %s", genre, page, r.status_code)
                break
            groups = r.json().get("release-groups", [])
            if not groups:
                break
            for g in groups:
                g["_query_genre"] = genre
                g["_ingested_at"] = now
                records.append(g)
    if not records:
        raise SourceError("musicbrainz", f"no release groups returned for genres {config.MB_GENRES}")
    key = bronze_key("musicbrainz", "release_groups", ds, batch_id, "release_groups.jsonl")
    n = storage.put_jsonl(key, records)
    log.info("MusicBrainz: wrote %s records to %s", n, storage.volume_path(key))
    return n
