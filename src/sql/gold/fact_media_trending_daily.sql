-- Gold fact of TMDB trending: one row per movie and day it was in trending/movie/day, with its rank.
-- Created if missing by: pipeline.py setup, after dim_date and dim_media.
-- Rebuilt on every run from silver (gold/load/fact_media_trending_daily.sql).
CREATE TABLE IF NOT EXISTS ${catalog}.gold.fact_media_trending_daily (
  date_key   INT     NOT NULL COMMENT 'snapshot day, yyyymmdd (gold.dim_date)',
  media_key  STRING  NOT NULL COMMENT 'gold.dim_media',
  rank       INT     NOT NULL COMMENT '1 = first in TMDB trending that day',
  CONSTRAINT fact_media_trending_daily_pk PRIMARY KEY (date_key, media_key),
  CONSTRAINT fact_media_trending_daily_date_fk FOREIGN KEY (date_key) REFERENCES ${catalog}.gold.dim_date,
  CONSTRAINT fact_media_trending_daily_media_fk FOREIGN KEY (media_key) REFERENCES ${catalog}.gold.dim_media
)
USING DELTA
LOCATION '${gold_root}/fact_media_trending_daily'
COMMENT 'Daily rank of the movies in TMDB trending, full load';
