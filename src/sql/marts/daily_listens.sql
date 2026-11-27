-- Mart view for the dashboard (Preset): ListenBrainz listens per day, in place of the plays per minute of the
-- streaming design. Created or replaced by: pipeline.py marts.
CREATE OR REPLACE VIEW ${catalog}.marts.daily_listens (
  date             COMMENT 'snapshot day',
  albums_measured  COMMENT 'albums with ListenBrainz listens that day',
  listens_gained   COMMENT 'listens since the previous snapshot, summed over the albums that had one',
  total_listens    COMMENT 'all-time listens of the albums measured that day'
)
COMMENT 'ListenBrainz listens per day (gold.fact_media_metric_daily)'
AS
SELECT
  d.date,
  count(*)                    AS albums_measured,
  sum(f.listen_count_change)  AS listens_gained,
  sum(f.listen_count)         AS total_listens
FROM ${catalog}.gold.fact_media_metric_daily AS f
JOIN ${catalog}.gold.dim_date AS d ON d.date_key = f.date_key
WHERE f.listen_count IS NOT NULL
GROUP BY d.date;
