"""Bronze writes under BRONZE_ROOT: data part files and batch manifests (layout: ingest/__init__.py).

On Databricks BRONZE_ROOT is a Unity Catalog volume mounted under /Volumes, so a write is a plain file write. From a
laptop, a /Volumes path goes through the Databricks Files API (databricks-sdk with `databricks auth login`), and any
other path is a local folder (tests, dry runs).
"""
import io
import json
import os
import posixpath

from common import config

MANIFESTS = "_batches"


def data_dir(source: str, entity: str, logical_date: str, batch_id: str) -> str:
    return f"{config.BRONZE_ROOT}/{source}/{entity}/dt={logical_date}/batch_id={batch_id}"


def manifest_path(source: str, logical_date: str, batch_id: str) -> str:
    return f"{config.BRONZE_ROOT}/{MANIFESTS}/{source}/dt={logical_date}/{batch_id}.json"


def _workspace():
    from databricks.sdk import WorkspaceClient
    return WorkspaceClient()


def put_bytes(path: str, body: bytes) -> str:
    if config.ON_DATABRICKS or not path.startswith("/Volumes/"):
        os.makedirs(posixpath.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(body)
    else:
        w = _workspace()
        w.files.create_directory(posixpath.dirname(path))  # idempotent, like mkdir -p
        w.files.upload(path, io.BytesIO(body), overwrite=True)
    return path


def write_manifest(batch) -> str:
    """One JSON line, so the manifests read as a table: read_files('<root>/_batches/*/*/*.json', format => 'json')."""
    body = (json.dumps(batch.manifest(), ensure_ascii=False) + "\n").encode("utf-8")
    return put_bytes(manifest_path(batch.source, batch.logical_date, batch.batch_id), body)


class BatchWriter:
    """Wraps each record in the bronze envelope and writes part files of at most BRONZE_MAX_RECORDS_PER_FILE lines,
    one series per entity. Records and files written are counted in batch.entities."""

    def __init__(self, batch, max_records: int = 0):
        self.batch = batch
        self.max_records = max_records or config.BRONZE_MAX_RECORDS_PER_FILE
        self._lines: dict = {}  # entity -> JSON lines not written yet

    def write(self, page) -> None:
        self.batch.entities.setdefault(page.entity, {"records": 0, "files": []})
        lines = self._lines.setdefault(page.entity, [])
        for record in page.records:
            lines.append(json.dumps(self._envelope(page, record), ensure_ascii=False))
            if len(lines) >= self.max_records:
                self._flush(page.entity)
                lines = self._lines[page.entity]

    def close(self) -> None:
        for entity in list(self._lines):
            self._flush(entity)

    def _envelope(self, page, record) -> dict:
        b = self.batch
        return {"_batch_id": b.batch_id, "_source": b.source, "_entity": page.entity,
                "_logical_date": b.logical_date, "_extracted_at": page.extracted_at, "_request": page.request,
                "payload": record}

    def _flush(self, entity: str) -> None:
        lines = self._lines.get(entity)
        if not lines:
            return
        stats = self.batch.entities[entity]
        name = f"part-{len(stats['files']):05d}.jsonl"
        body = ("\n".join(lines) + "\n").encode("utf-8")
        b = self.batch
        put_bytes(f"{data_dir(b.source, entity, b.logical_date, b.batch_id)}/{name}", body)
        stats["records"] += len(lines)
        stats["files"].append({"name": name, "records": len(lines), "bytes": len(body)})
        self._lines[entity] = []
