"""One ingest batch = one run of one source: the id, times, counters and status that make its manifest.

batch_id      <start time, UTC>-<6 random hex>, e.g. 20261126T013005Z-7f3a9c. Sorts by start time and stays unique
              for two runs in the same second.
logical_date  the day the data belongs to (--date, folder dt=); started_at / finished_at are when the batch ran.
              All times are UTC, ISO-8601 with a Z.
status        RUNNING   written at the start; still RUNNING after the task timeout means the run was killed
              SUCCESS   every page fetched
              PARTIAL   data written, but some pages failed (pages_failed, failures)
              FAILED    the task fails and Databricks retries it, as a new batch
              SKIPPED   nothing to fetch (no TMDB API key)
"""
import logging
import os
import secrets
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from common import config
from common.errors import redact

log = logging.getLogger(__name__)

RUNNING, SUCCESS, PARTIAL, FAILED, SKIPPED = "RUNNING", "SUCCESS", "PARTIAL", "FAILED", "SKIPPED"
MAX_FAILURES_KEPT = 20  # failed pages listed in the manifest; pages_failed counts all of them
_VERSION_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "VERSION")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso(t: datetime) -> str:
    """2026-11-26T01:30:05.123Z"""
    return t.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def new_batch_id(started_at: datetime) -> str:
    return f"{started_at.astimezone(timezone.utc):%Y%m%dT%H%M%SZ}-{secrets.token_hex(3)}"


def code_version() -> Optional[str]:
    """Commit of the deployed code: setting CODE_VERSION, else the file src/VERSION, else None."""
    if config.CODE_VERSION:
        return config.CODE_VERSION
    try:
        with open(_VERSION_FILE, encoding="utf-8") as f:
            return f.read().strip() or None
    except OSError:
        return None


@dataclass
class Page:
    """Records of one response (or file) of a source, written to bronze as they are."""
    entity: str  # folder under the source, e.g. movies
    records: list
    request: dict  # what was asked, e.g. {"endpoint": "movie/popular", "page": 3}
    extracted_at: str = field(default_factory=lambda: iso(utc_now()))


class HttpStats:
    """HTTP attempts of a requests session, counted by the hook on_response: each final response plus the retries
    urllib3 made before it (429, 5xx). A request that exhausts its retries raises instead; the source records it
    as a failed page."""

    def __init__(self):
        self.requests = 0
        self.retries = 0
        self.status = Counter()

    def on_response(self, response, *args, **kwargs) -> None:
        history = getattr(getattr(getattr(response, "raw", None), "retries", None), "history", ())
        if not isinstance(history, tuple):
            history = ()
        for attempt in history:
            self.status[str(attempt.status or "error")] += 1
        self.retries += len(history)
        self.requests += len(history) + 1
        self.status[str(response.status_code)] += 1

    def as_dict(self) -> dict:
        return {"requests": self.requests, "retries": self.retries, "status": dict(sorted(self.status.items()))}


class Batch:
    def __init__(self, source: str, logical_date: str, params: Optional[dict] = None,
                 started_at: Optional[datetime] = None):
        self.source = source
        self.logical_date = logical_date
        self.params = params or {}
        self.started_at = started_at or utc_now()
        self.batch_id = new_batch_id(self.started_at)
        self.status = RUNNING
        self.finished_at: Optional[datetime] = None
        self.error: Optional[dict] = None
        self.entities: dict = {}  # entity -> {"records": n, "files": [{"name", "records", "bytes"}]}, by bronze.py
        self.http = HttpStats()
        self.pages_failed = 0
        self.failures: list = []

    @property
    def records(self) -> int:
        return sum(e["records"] for e in self.entities.values())

    def fail_page(self, request: dict, reason: str) -> None:
        """A page the source could not get. The batch goes on and ends PARTIAL."""
        reason = redact(reason)
        self.pages_failed += 1
        if len(self.failures) < MAX_FAILURES_KEPT:
            self.failures.append({"request": request, "reason": reason})
        log.warning("%s %s failed: %s", self.source, request, reason)

    def finish(self, status: str, error: Optional[BaseException] = None) -> None:
        self.status = status
        self.finished_at = utc_now()
        if error is not None:
            self.error = {"type": type(error).__name__, "message": redact(error)[:2000]}

    def manifest(self) -> dict:
        end = self.finished_at
        return {
            "batch_id": self.batch_id,
            "source": self.source,
            "logical_date": self.logical_date,
            "status": self.status,
            "started_at": iso(self.started_at),
            "finished_at": iso(end) if end else None,
            "duration_s": round((end - self.started_at).total_seconds(), 3) if end else None,
            "trigger": config.JOB_TRIGGER or None,
            "databricks": {"job_id": config.JOB_ID or None, "job_run_id": config.JOB_RUN_ID or None,
                           "task_run_id": config.TASK_RUN_ID or None},
            "code_version": code_version(),
            "params": self.params,
            "entities": self.entities,
            "records": self.records,
            "http": self.http.as_dict(),
            "pages_failed": self.pages_failed,
            "failures": self.failures,
            "error": self.error,
        }
