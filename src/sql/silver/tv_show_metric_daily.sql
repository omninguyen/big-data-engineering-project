-- Silver table of daily TMDB metrics of TV shows: one row per show and snapshot date.
-- Created if missing by: pipeline.py setup. Loaded batch by batch from bronze.tmdb_tv_shows
-- (silver/load/tv_show_metric_daily.sql).
CREATE TABLE IF NOT EXISTS ${catalog}.silver.tv_show_metric_daily (
  tv_id          BIGINT     NOT NULL COMMENT 'TMDB TV id (silver.tv_show)',
  snapshot_date  DATE       NOT NULL COMMENT 'ingest date (bronze dt)',
  popularity     DOUBLE,
  vote_average   DOUBLE              COMMENT '0-10',
  vote_count     INT,
  batch_id       STRING     NOT NULL COMMENT 'bronze batch of these values: the latest of the day',
  _loaded_at     TIMESTAMP  NOT NULL COMMENT 'when this row was last written to silver',
  CONSTRAINT tv_show_metric_daily_pk PRIMARY KEY (tv_id, snapshot_date)
)
USING DELTA
LOCATION '${silver_root}/tv_show_metric_daily'
COMMENT 'Daily TMDB metrics per TV show (incremental), from bronze.tmdb_tv_shows';
