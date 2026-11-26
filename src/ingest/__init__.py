"""Ingest: one batch of a source API -> raw files in the ADLS container bronze, plus a manifest for monitoring.

Bronze layout (BRONZE_ROOT, the external volume <catalog>.bronze.landing on abfss://bronze@<account>.../landing):

  <source>/<entity>/dt=<logical date>/batch_id=<batch id>/part-00000.jsonl   data, append-only
  _batches/<source>/dt=<logical date>/<batch id>.json                       manifest, one JSON line per batch

Each data line: {"_batch_id", "_source", "_entity", "_logical_date", "_extracted_at", "_request", "payload"},
with payload the source record unchanged. A rerun (or a Databricks retry) is a new batch, so data files are never
overwritten. The manifest is written as RUNNING at the start and again with the final status; ids, times and
statuses are described in ingest/batch.py.

  batch.py     Batch (id, times, counters, status -> manifest), Page, HttpStats
  bronze.py    paths, part files, manifest writes
  runner.py    run(source, logical_date): one batch from start to manifest
  sources/     one module per source: params() and extract(batch) -> Page, ...
"""
