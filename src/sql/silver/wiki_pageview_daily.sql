-- Silver table of daily Wikipedia pageviews: one row per article and day.
-- Created if missing by: pipeline.py setup. Loaded batch by batch from bronze.wikipedia_pageviews
-- (silver/load/wiki_pageview_daily.sql).
CREATE TABLE IF NOT EXISTS ${catalog}.silver.wiki_pageview_daily (
  article     STRING     NOT NULL COMMENT 'Wikipedia article title with underscores (silver.media_wiki_page)',
  view_date   DATE       NOT NULL COMMENT 'day of the views (UTC day, as Wikimedia counts them)',
  views       BIGINT     NOT NULL COMMENT 'views by people that day',
  batch_id    STRING     NOT NULL COMMENT 'bronze batch of this value: the latest one holding the day',
  _loaded_at  TIMESTAMP  NOT NULL COMMENT 'when this row was last written to silver',
  CONSTRAINT wiki_pageview_daily_pk PRIMARY KEY (article, view_date)
)
USING DELTA
LOCATION '${silver_root}/wiki_pageview_daily'
COMMENT 'Daily Wikipedia pageviews per article (incremental), from bronze.wikipedia_pageviews';
