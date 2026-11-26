"""Batch ids: every ingest run writes into its own folder, so rerunning a date never overwrites an earlier run.

  bronze/<source>/<entity>/dt=<date>/batch_id=<batch id>/<file>

dt and the batch id both use Vietnam time, the timezone of the job's schedule, so a run's batch id starts with its
date: dt=2026-11-27/batch_id=20261127T020005-7f3a9c is the run that started at 02:00:05 on 27/11.
"""
import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional

# Asia/Ho_Chi_Minh (no daylight saving time), as timezone_id of the schedule in databricks.yml
VIETNAM = timezone(timedelta(hours=7), "Asia/Ho_Chi_Minh")


def today() -> str:
    """Today in Vietnam time, YYYY-MM-DD: the default dt."""
    return datetime.now(VIETNAM).date().isoformat()


def new_batch_id(started_at: Optional[datetime] = None) -> str:
    """<start time, Vietnam time>-<6 random hex>, e.g. 20261127T013005-7f3a9c: sorts by start time (Vietnam has no
    daylight saving time) and is unique within a second."""
    t = (started_at or datetime.now(VIETNAM)).astimezone(VIETNAM)
    return f"{t:%Y%m%dT%H%M%S}-{secrets.token_hex(3)}"


def bronze_key(source: str, entity: str, ds: str, batch_id: str, name: str) -> str:
    """Storage key of one file of a batch, e.g. bronze/tmdb/movies/dt=2026-11-27/batch_id=<id>/movies.jsonl"""
    return f"bronze/{source}/{entity}/dt={ds}/batch_id={batch_id}/{name}"
