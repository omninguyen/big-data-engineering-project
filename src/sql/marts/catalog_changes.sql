-- Mart view for the dashboard (Preset): how many movies, TV shows and albums came in or changed each day.
-- Created or replaced by: pipeline.py marts.
CREATE OR REPLACE VIEW ${catalog}.marts.catalog_changes (
  date           COMMENT 'day of the changes (Vietnam time)',
  media_type     COMMENT 'movie | tv | music',
  new_items      COMMENT 'media seen for the first time that day (the first run of a source counts its whole catalog)',
  changed_items  COMMENT 'media whose details changed that day (a new SCD2 version in silver)'
)
COMMENT 'New and changed movies, TV shows and albums per day (gold.fact_media_change)'
AS
SELECT
  d.date,
  m.media_type,
  count(DISTINCT CASE WHEN c.change_type = 'new' THEN c.media_key END)      AS new_items,
  count(DISTINCT CASE WHEN c.change_type = 'changed' THEN c.media_key END)  AS changed_items
FROM ${catalog}.gold.fact_media_change AS c
JOIN ${catalog}.gold.dim_media AS m ON m.media_key = c.media_key
JOIN ${catalog}.gold.dim_date AS d ON d.date_key = c.date_key
GROUP BY d.date, m.media_type;
