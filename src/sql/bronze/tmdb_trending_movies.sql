-- Bronze table of the raw TMDB trending files (landing/tmdb/trending_movies/): the rank of each movie of
-- trending/movie/day at ingest time. Its details are in bronze.tmdb_movies (endpoint trending/movie/day).
-- Created if missing by: pipeline.py setup
CREATE TABLE IF NOT EXISTS ${catalog}.bronze.tmdb_trending_movies (
  id                 STRING     COMMENT 'TMDB movie id',
  rank               INT        COMMENT '1 = first in trending/movie/day, in TMDB order',
  dt                 DATE       COMMENT 'ingest date, from the dt= folder',
  batch_id           STRING     COMMENT 'ingest run, from the batch_id= folder',
  _source_file       STRING     COMMENT 'path of the delivered file',
  _ingested_at       TIMESTAMP  COMMENT 'when the API was called (from the file)',
  _file_size         BIGINT     COMMENT 'size in bytes of the delivered file',
  _file_modified_at  TIMESTAMP  COMMENT 'when the delivered file was last written'
)
USING DELTA
LOCATION '${tables_root}/tmdb_trending_movies'
COMMENT 'Raw TMDB trending movies of the day: one row per movie and batch, with its rank';
