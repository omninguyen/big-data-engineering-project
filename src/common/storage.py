"""Raw (bronze) files of the ingest tasks, written straight into landing/ of the ADLS container bronze.

Key 'bronze/<rest>' -> '<BRONZE_ROOT>/<rest>',
e.g. abfss://bronze@<account>.dfs.core.windows.net/landing/tmdb/movies/...
On Databricks, abfss:// paths go through dbutils.fs, which uses the Unity Catalog external location of the container
(no key or secret in the code). BRONZE_ROOT may also be a local folder, to try an ingest on a laptop.
"""
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


def bronze_path(key: str) -> str:
    if not key.startswith(BRONZE):
        raise ValueError(f"only bronze/ keys are written by the ingest tasks, got {key!r}")
    if not config.BRONZE_ROOT:
        raise ValueError("BRONZE_ROOT is not set: pass "
                         "--set BRONZE_ROOT=abfss://bronze@<account>.dfs.core.windows.net/landing")
    return f"{config.BRONZE_ROOT}/{key[len(BRONZE):]}"


def _in_cloud(path: str) -> bool:
    return path.startswith("abfss://")


def _dbutils():
    from databricks.sdk.runtime import dbutils  # preinstalled on Databricks runtimes
    return dbutils


def _fs():
    if not config.ON_DATABRICKS:
        raise RuntimeError("abfss:// paths are only reachable on Databricks; "
                           "set BRONZE_ROOT to a local folder to run the ingest elsewhere")
    return _dbutils().fs


def put_bytes(key: str, body: bytes) -> str:
    path = bronze_path(key)
    if _in_cloud(path):
        fs = _fs()
        fs.mkdirs(posixpath.dirname(path))
        fs.put(path, body.decode("utf-8"), overwrite=True)
    else:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(body)
    return path


def put_jsonl(key: str, records: Iterable[dict]) -> int:
    body, n = jsonl_bytes(records)
    put_bytes(key, body)
    return n


def read_text(path: str) -> str:
    """A whole text file, on ADLS (abfss://) or local. A missing local file raises FileNotFoundError."""
    if _in_cloud(path):
        fs = _fs()
        size = fs.ls(path)[0].size
        return fs.head(path, size) if size else ""
    with open(path, encoding="utf-8") as f:
        return f.read()
