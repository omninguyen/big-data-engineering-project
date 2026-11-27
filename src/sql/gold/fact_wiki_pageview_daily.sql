-- Gold fact of daily Wikipedia pageviews: one row per movie or album with an article and day.
-- Created if missing by: pipeline.py setup, after dim_date, dim_media and dim_wiki_page.
-- Rebuilt on every run from silver (gold/load/fact_wiki_pageview_daily.sql).
CREATE TABLE IF NOT EXISTS ${catalog}.gold.fact_wiki_pageview_daily (
  date_key   INT     NOT NULL COMMENT 'day of the views, yyyymmdd (gold.dim_date); a UTC day, as Wikimedia counts',
  media_key  STRING  NOT NULL COMMENT 'gold.dim_media',
  views      BIGINT  NOT NULL COMMENT 'views of its Wikipedia article by people that day',
  CONSTRAINT fact_wiki_pageview_daily_pk PRIMARY KEY (date_key, media_key),
  CONSTRAINT fact_wiki_pageview_daily_date_fk FOREIGN KEY (date_key) REFERENCES ${catalog}.gold.dim_date,
  CONSTRAINT fact_wiki_pageview_daily_media_fk FOREIGN KEY (media_key) REFERENCES ${catalog}.gold.dim_media
)
USING DELTA
LOCATION '${gold_root}/fact_wiki_pageview_daily'
COMMENT 'Daily Wikipedia pageviews of movies and albums, full load';
