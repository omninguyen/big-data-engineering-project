-- Silver table of daily TMDB metrics: one row per movie and snapshot date.
-- Created if missing by: pipeline.py setup. Loaded batch by batch from bronze.tmdb_movies
-- (silver/load/movie_metric_daily.sql).
CREATE TABLE IF NOT EXISTS ${catalog}.silver.movie_metric_daily (
  movie_id       BIGINT     NOT NULL COMMENT 'TMDB id',
  snapshot_date  DATE       NOT NULL COMMENT 'ingest date (bronze dt)',
  popularity     DOUBLE,
  vote_average   DOUBLE              COMMENT '0-10',
  vote_count     INT,
  in_popular     BOOLEAN    NOT NULL COMMENT 'in movie/popular that day',
  in_top_rated   BOOLEAN    NOT NULL COMMENT 'in movie/top_rated that day',
  batch_id       STRING     NOT NULL COMMENT 'bronze batch of these values: the latest of the day',
  _loaded_at     TIMESTAMP  NOT NULL COMMENT 'when this row was last written to silver',
  CONSTRAINT movie_metric_daily_pk PRIMARY KEY (movie_id, snapshot_date)
)
USING DELTA
LOCATION '${silver_root}/movie_metric_daily'
COMMENT 'Daily TMDB metrics per movie (incremental), from bronze.tmdb_movies';
