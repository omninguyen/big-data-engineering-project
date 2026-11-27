-- Silver table of TMDB's trending movies: one row per movie and day it was in trending/movie/day.
-- Created if missing by: pipeline.py setup. Loaded batch by batch from bronze.tmdb_trending_movies
-- (silver/load/movie_trending_daily.sql).
CREATE TABLE IF NOT EXISTS ${catalog}.silver.movie_trending_daily (
  movie_id       BIGINT     NOT NULL COMMENT 'TMDB id (silver.movie)',
  snapshot_date  DATE       NOT NULL COMMENT 'ingest date (bronze dt)',
  rank           INT        NOT NULL COMMENT '1 = first in trending/movie/day',
  batch_id       STRING     NOT NULL COMMENT 'bronze batch of this rank: the latest of the day holding the movie',
  _loaded_at     TIMESTAMP  NOT NULL COMMENT 'when this row was last written to silver',
  CONSTRAINT movie_trending_daily_pk PRIMARY KEY (movie_id, snapshot_date)
)
USING DELTA
LOCATION '${silver_root}/movie_trending_daily'
COMMENT 'Daily rank of the movies in TMDB trending (incremental), from bronze.tmdb_trending_movies';
