-- Mart view for the dashboard (Preset): the latest batch of every source and how fresh it is.
-- Times are shown in Vietnam time (from_utc_timestamp, for the UTC session time zone of Databricks).
-- Created or replaced by: pipeline.py marts.
CREATE OR REPLACE VIEW ${catalog}.marts.freshness (
  dataset                COMMENT 'bronze table of the source',
  last_batch_id          COMMENT 'latest ingest batch',
  last_dt                COMMENT 'day of the latest batch',
  last_ingested_at       COMMENT 'when the latest batch was fetched from the source API (Vietnam time)',
  hours_since_ingest     COMMENT 'hours from last_ingested_at to now',
  last_silver_loaded_at  COMMENT 'when the latest batch reached silver (Vietnam time), NULL: not yet',
  silver_lag_s           COMMENT 'seconds from ingest to silver of the latest batch',
  status                 COMMENT 'fresh: in silver, ingested within 26 hours (daily run + margin) | stale | not_loaded'
)
COMMENT 'Latest batch of every source and how fresh it is (ops.pipeline_record_metrics)'
AS
WITH latest AS (  -- batch ids start with their time, so the greatest is the latest
  SELECT *
  FROM (
    SELECT *, row_number() OVER (PARTITION BY dataset ORDER BY batch_id DESC) AS rn
    FROM ${catalog}.ops.pipeline_record_metrics
  )
  WHERE rn = 1
)
SELECT
  dataset,
  batch_id                                                                       AS last_batch_id,
  dt                                                                             AS last_dt,
  from_utc_timestamp(last_ingested_at, 'Asia/Ho_Chi_Minh')                       AS last_ingested_at,
  round(CAST(timestampdiff(MINUTE, last_ingested_at, current_timestamp()) AS DOUBLE) / 60, 1)
                                                                                 AS hours_since_ingest,
  from_utc_timestamp(silver_loaded_at, 'Asia/Ho_Chi_Minh')                       AS last_silver_loaded_at,
  silver_lag_s,
  CASE
    WHEN silver_loaded_at IS NULL THEN 'not_loaded'
    WHEN timestampdiff(MINUTE, last_ingested_at, current_timestamp()) > 26 * 60 THEN 'stale'
    ELSE 'fresh'
  END                                                                            AS status
FROM latest;
