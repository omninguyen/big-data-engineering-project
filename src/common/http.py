from typing import Callable, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def session(user_agent: str, proxy: str = "", on_response: Optional[Callable] = None) -> requests.Session:
    """HTTP session with retries (GET only, honours Retry-After). `proxy` (e.g. http://proxy.local:7890) applies to
    this session only, so other traffic of the same process (Unity Catalog, MusicBrainz) is never routed through it.
    `on_response` is called with every response (ingest.batch.HttpStats counts requests, retries and status codes)."""
    s = requests.Session()
    if proxy:
        s.proxies.update({"http": proxy, "https": proxy})
    retry = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=["GET"], respect_retry_after_header=True)
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({"User-Agent": user_agent})
    if on_response is not None:
        s.hooks["response"].append(on_response)
    return s
