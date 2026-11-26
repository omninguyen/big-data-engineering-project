"""MusicBrainz: release groups (albums) per genre tag -> entity release_groups.

Rate limit: 1 request/second (we sleep 1.1 s). A contact in the User-Agent is required (MB_CONTACT; it is not
copied into the manifest). A failing genre page ends that genre and is counted, batch PARTIAL. No album at all
(network down, every request rejected) -> SourceError, batch FAILED.
"""
import time

import requests

from common import config
from common.errors import SourceError
from common.http import session
from ingest.batch import Page

URL = "https://musicbrainz.org/ws/2/release-group"
PAGE_SIZE = 100


def params() -> dict:
    return {"genres": config.MB_GENRES, "pages_per_genre": config.MB_PAGES_PER_GENRE, "page_size": PAGE_SIZE}


def extract(batch):
    sess = session(f"MediaPlatformThesis/1.0 ( {config.MB_CONTACT} )", on_response=batch.http.on_response)
    albums = 0
    for genre in config.MB_GENRES:
        for page in range(config.MB_PAGES_PER_GENRE):
            request = {"query_genre": genre, "offset": page * PAGE_SIZE}
            query = {"query": f'tag:"{genre}" AND primarytype:album', "fmt": "json",
                     "limit": PAGE_SIZE, "offset": page * PAGE_SIZE}
            try:
                r = sess.get(URL, params=query, timeout=30)
            except requests.RequestException as e:
                batch.fail_page(request, type(e).__name__)
                break
            finally:
                time.sleep(1.1)
            if r.status_code != 200:
                batch.fail_page(request, f"HTTP {r.status_code}")
                break
            groups = r.json().get("release-groups", [])
            if not groups:
                break
            albums += len(groups)
            yield Page("release_groups", groups, request)
    if not albums:
        raise SourceError("musicbrainz", f"no release groups returned for genres {config.MB_GENRES}")
