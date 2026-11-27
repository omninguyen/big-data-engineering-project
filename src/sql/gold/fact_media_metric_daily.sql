-- Gold fact of daily media metrics: one row per movie or album and snapshot day. Movies carry the TMDB columns,
-- albums the ListenBrainz columns; the columns of the other media type are NULL.
-- Created if missing by: pipeline.py setup, after dim_date and dim_media (its foreign keys).
-- Rebuilt on every run from silver (gold/load/fact_media_metric_daily.sql).
CREATE TABLE IF NOT EXISTS ${catalog}.gold.fact_media_metric_daily (
  date_key             INT     NOT NULL COMMENT 'snapshot day, yyyymmdd (gold.dim_date)',
  media_key            STRING  NOT NULL COMMENT 'gold.dim_media',
  popularity           DOUBLE           COMMENT 'TMDB popularity score of that day (movies, TV shows)',
  rating               DOUBLE           COMMENT 'TMDB vote average 0-10 (movies, TV shows)',
  votes                INT              COMMENT 'TMDB vote count, all-time total (movies, TV shows)',
  listen_count         BIGINT           COMMENT 'ListenBrainz listens, all-time total (music)',
  listener_count       BIGINT           COMMENT 'ListenBrainz listeners, all-time total (music)',
  listen_count_change  BIGINT           COMMENT 'listens since the previous snapshot of the album, NULL on the first',
  CONSTRAINT fact_media_metric_daily_pk PRIMARY KEY (date_key, media_key),
  CONSTRAINT fact_media_metric_daily_date_fk FOREIGN KEY (date_key) REFERENCES ${catalog}.gold.dim_date,
  CONSTRAINT fact_media_metric_daily_media_fk FOREIGN KEY (media_key) REFERENCES ${catalog}.gold.dim_media
)
USING DELTA
LOCATION '${gold_root}/fact_media_metric_daily'
COMMENT 'Daily metrics of movies and TV shows (TMDB) and albums (ListenBrainz), full load';
