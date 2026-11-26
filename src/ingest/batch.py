"""Batch ids: every ingest run writes into its own folder, so rerunning a date never overwrites an earlier run.

  bronze/<source>/<entity>/dt=<date>/batch_id=<batch id>/<file>
"""
import secrets
from datetime import datetime, timezone
from typing import Optional


def new_batch_id(started_at: Optional[datetime] = None) -> str:
    """<start time, UTC>-<6 random hex>, e.g. 20261126T013005Z-7f3a9c: sorts by start time, unique within a second."""
    t = (started_at or datetime.now(timezone.utc)).astimezone(timezone.utc)
    return f"{t:%Y%m%dT%H%M%SZ}-{secrets.token_hex(3)}"


def bronze_key(source: str, entity: str, ds: str, batch_id: str, name: str) -> str:
    """Storage key of one file of a batch, e.g. bronze/tmdb/movies/dt=2026-11-26/batch_id=<id>/movies.jsonl"""
    return f"bronze/{source}/{entity}/dt={ds}/batch_id={batch_id}/{name}"
