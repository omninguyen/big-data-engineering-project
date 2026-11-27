-- Mart view for the dashboard (Preset): Wikipedia views per day and media type. Created or replaced by:
-- pipeline.py marts.
CREATE OR REPLACE VIEW ${catalog}.marts.daily_wiki_views (
  date        COMMENT 'day of the views (UTC day, as Wikimedia counts them)',
  media_type  COMMENT 'movie | music',
  items       COMMENT 'movies or albums whose article had views that day',
  views       COMMENT 'Wikipedia views of their articles that day'
)
COMMENT 'Wikipedia views per day and media type (gold.fact_wiki_pageview_daily)'
AS
SELECT d.date, m.media_type, count(*) AS items, sum(f.views) AS views
FROM ${catalog}.gold.fact_wiki_pageview_daily AS f
JOIN ${catalog}.gold.dim_media AS m ON m.media_key = f.media_key
JOIN ${catalog}.gold.dim_date AS d ON d.date_key = f.date_key
GROUP BY d.date, m.media_type;
