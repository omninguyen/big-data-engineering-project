-- Mart view for the dashboard (Preset): the quality checks of the latest batch of every source.
-- Created or replaced by: pipeline.py marts.
CREATE OR REPLACE VIEW ${catalog}.marts.latest_quality_checks (
  dataset      COMMENT 'bronze table of the source',
  batch_id     COMMENT 'latest ingest batch of the source',
  dt           COMMENT 'day of the batch',
  check_name   COMMENT 'rejected_rate | duplicate_rate | loaded_to_silver',
  severity     COMMENT 'critical | warning',
  value        COMMENT 'measured value',
  op           COMMENT '<= or >=: passed when value <op> threshold',
  threshold    COMMENT 'threshold of the check',
  passed       COMMENT 'true when the check passed',
  description  COMMENT 'what the check measures'
)
COMMENT 'Quality checks of the latest batch of every source (ops.data_quality_results)'
AS
WITH latest AS (  -- batch ids start with their time, so the greatest is the latest
  SELECT dataset, batch_id, dt
  FROM (
    SELECT dataset, batch_id, dt, row_number() OVER (PARTITION BY dataset ORDER BY batch_id DESC) AS rn
    FROM ${catalog}.ops.pipeline_record_metrics
  )
  WHERE rn = 1
)
SELECT q.dataset, q.batch_id, l.dt, q.check_name, q.severity, q.value, q.op, q.threshold, q.passed, q.description
FROM ${catalog}.ops.data_quality_results AS q
JOIN latest AS l ON l.dataset = q.dataset AND l.batch_id = q.batch_id;
