-- Ops table of record metrics: one row per bronze table and ingest batch (one run of a source): how many rows came
-- in, how many silver takes, the delivered files, and how long the batch took to reach silver.
-- Created if missing by: pipeline.py setup. Rebuilt on every run from bronze and silver.load_log
-- (ops/load/pipeline_record_metrics.sql).
CREATE TABLE IF NOT EXISTS ${catalog}.ops.pipeline_record_metrics (
  dataset                STRING     NOT NULL COMMENT 'bronze table that was read',
  target                 STRING     NOT NULL COMMENT 'main silver table it feeds',
  batch_id               STRING     NOT NULL COMMENT 'ingest batch, <yyyymmddThhmmss>-<6 hex> in Vietnam time',
  dt                     DATE                COMMENT 'day of the batch',
  rows_read              BIGINT     NOT NULL COMMENT 'rows of the batch in bronze',
  rows_valid             BIGINT     NOT NULL COMMENT 'rows passing the silver load filters (silver/load/<target>.sql)',
  rows_rejected          BIGINT     NOT NULL COMMENT 'rows_read - rows_valid',
  rows_duplicate         BIGINT     NOT NULL COMMENT 'valid rows dropped as duplicates of the same key',
  files                  BIGINT     NOT NULL COMMENT 'delivered files',
  bytes                  BIGINT              COMMENT 'total size of the delivered files',
  last_file_modified_at  TIMESTAMP           COMMENT 'newest delivered file',
  last_ingested_at       TIMESTAMP           COMMENT 'when the ingest fetched the newest row from the source API',
  silver_loaded_at       TIMESTAMP           COMMENT 'when the batch reached target (silver.load_log), NULL: not yet',
  silver_lag_s           BIGINT              COMMENT 'seconds from last_ingested_at to silver_loaded_at',
  CONSTRAINT pipeline_record_metrics_pk PRIMARY KEY (dataset, batch_id)
)
USING DELTA
LOCATION '${ops_root}/pipeline_record_metrics'
COMMENT 'Rows, files and lag of every ingest batch, full load';
