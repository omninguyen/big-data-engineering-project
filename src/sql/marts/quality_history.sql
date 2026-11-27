-- Mart view for the dashboard (Preset): rows, files, lag and failed checks per day and source. Rates are left to the
-- dashboard as sums of these counts (1 - SUM(rows_rejected) / SUM(rows_read)), so they stay right across days.
-- Created or replaced by: pipeline.py marts.
CREATE OR REPLACE VIEW ${catalog}.marts.quality_history (
  date             COMMENT 'day of the batches',
  dataset          COMMENT 'bronze table of the source',
  batches          COMMENT 'ingest batches that day (runs of the source)',
  rows_read        COMMENT 'rows delivered',
  rows_valid       COMMENT 'rows passing the silver load filters',
  rows_rejected    COMMENT 'rows left out of silver',
  rows_duplicate   COMMENT 'valid rows dropped as duplicates of the same key',
  files            COMMENT 'delivered files',
  bytes            COMMENT 'total size of the delivered files',
  silver_lag_s     COMMENT 'longest seconds from ingest to silver among the batches',
  failed_critical  COMMENT 'critical checks failed',
  failed_warning   COMMENT 'warning checks failed'
)
COMMENT 'Rows, files, lag and failed checks per day and source (ops.pipeline_record_metrics, ops.data_quality_results)'
AS
WITH checks AS (
  SELECT
    dataset, batch_id,
    count_if(NOT passed AND severity = 'critical')  AS failed_critical,
    count_if(NOT passed AND severity = 'warning')   AS failed_warning
  FROM ${catalog}.ops.data_quality_results
  GROUP BY dataset, batch_id
)
SELECT
  r.dt                                AS date,
  r.dataset,
  count(*)                            AS batches,
  sum(r.rows_read)                    AS rows_read,
  sum(r.rows_valid)                   AS rows_valid,
  sum(r.rows_rejected)                AS rows_rejected,
  sum(r.rows_duplicate)               AS rows_duplicate,
  sum(r.files)                        AS files,
  sum(r.bytes)                        AS bytes,
  max(r.silver_lag_s)                 AS silver_lag_s,
  coalesce(sum(c.failed_critical), 0) AS failed_critical,
  coalesce(sum(c.failed_warning), 0)  AS failed_warning
FROM ${catalog}.ops.pipeline_record_metrics AS r
LEFT JOIN checks AS c ON c.dataset = r.dataset AND c.batch_id = r.batch_id
WHERE r.dt IS NOT NULL
GROUP BY r.dt, r.dataset;
