"""Raw (bronze) file writes for the ingest tasks: the Unity Catalog landing volume UC_LANDING_ROOT.

Key 'bronze/<rest>' -> '<UC_LANDING_ROOT>/<rest>'. On Databricks compute the volume is mounted under /Volumes,
so it is a plain file write. From a laptop (debugging an ingest module), the Databricks Files API is used:
databricks-sdk with DATABRICKS_HOST + DATABRICKS_TOKEN, or `databricks auth login`.
"""
import io
import json
import os
import posixpath
from typing import Iterable

from common import config

BRONZE = "bronze/"


def jsonl_bytes(records: Iterable[dict]):
    """(body, number of records): one JSON object per line, UTF-8."""
    lines = [json.dumps(r, ensure_ascii=False) for r in records]
    return (("\n".join(lines) + "\n").encode("utf-8") if lines else b""), len(lines)


def volume_path(key: str) -> str:
    if not key.startswith(BRONZE):
        raise ValueError(f"only bronze/ keys live in the landing volume, got {key!r}")
    return f"{config.UC_LANDING_ROOT}/{key[len(BRONZE):]}"


def _workspace():
    from databricks.sdk import WorkspaceClient
    return WorkspaceClient()


def put_bytes(key: str, body: bytes) -> str:
    path = volume_path(key)
    if config.ON_DATABRICKS:
        os.makedirs(posixpath.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(body)
    else:
        w = _workspace()
        w.files.create_directory(posixpath.dirname(path))  # idempotent, like mkdir -p
        w.files.upload(path, io.BytesIO(body), overwrite=True)
    return path


def put_jsonl(key: str, records: Iterable[dict]) -> int:
    body, n = jsonl_bytes(records)
    put_bytes(key, body)
    return n
