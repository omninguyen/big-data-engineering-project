-- Ops table of data quality checks: one row per check of every ingest batch. A check compares a measured value with
-- a threshold; critical checks point at data silver cannot use, warnings at data worth a look.
-- Created if missing by: pipeline.py setup, after pipeline_record_metrics (its foreign key).
-- Rebuilt on every run from ops.pipeline_record_metrics (ops/load/data_quality_results.sql).
CREATE TABLE IF NOT EXISTS ${catalog}.ops.data_quality_results (
  dataset      STRING   NOT NULL COMMENT 'bronze table that was checked',
  batch_id     STRING   NOT NULL COMMENT 'ingest batch',
  check_name   STRING   NOT NULL COMMENT 'rejected_rate | duplicate_rate | loaded_to_silver',
  severity     STRING   NOT NULL COMMENT 'critical | warning',
  value        DOUBLE   NOT NULL COMMENT 'measured value',
  op           STRING   NOT NULL COMMENT '<= or >=: passed when value <op> threshold',
  threshold    DOUBLE   NOT NULL,
  passed       BOOLEAN  NOT NULL,
  description  STRING   NOT NULL,
  CONSTRAINT data_quality_results_pk PRIMARY KEY (dataset, batch_id, check_name),
  CONSTRAINT data_quality_results_batch_fk FOREIGN KEY (dataset, batch_id)
    REFERENCES ${catalog}.ops.pipeline_record_metrics
)
USING DELTA
LOCATION '${ops_root}/data_quality_results'
COMMENT 'Data quality checks of every ingest batch, full load';
