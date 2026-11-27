-- Bronze table of the raw TMDB TV show files (landing/tmdb/tv_shows/): the series of tv/popular.
-- Created if missing by: pipeline.py setup
CREATE TABLE IF NOT EXISTS ${catalog}.bronze.tmdb_tv_shows (
  id                 STRING         COMMENT 'TMDB TV id',
  name               STRING,
  original_name      STRING,
  overview           STRING,
  first_air_date     STRING         COMMENT 'as delivered (YYYY-MM-DD)',
  genres             ARRAY<STRING>  COMMENT 'genre names, mapped from genre_ids with genre/tv/list at ingest',
  genre_ids          ARRAY<BIGINT>,
  popularity         DOUBLE,
  vote_average       DOUBLE,
  vote_count         BIGINT,
  original_language  STRING,
  origin_country     ARRAY<STRING>  COMMENT 'ISO 3166-1 codes',
  poster_path        STRING,
  adult              BOOLEAN,
  endpoint           STRING         COMMENT 'tv/popular',
  dt                 DATE           COMMENT 'ingest date, from the dt= folder',
  batch_id           STRING         COMMENT 'ingest run, from the batch_id= folder',
  _source_file       STRING         COMMENT 'path of the delivered file',
  _ingested_at       TIMESTAMP      COMMENT 'when the API was called (from the file)',
  _file_size         BIGINT         COMMENT 'size in bytes of the delivered file',
  _file_modified_at  TIMESTAMP      COMMENT 'when the delivered file was last written'
)
USING DELTA
LOCATION '${tables_root}/tmdb_tv_shows'
COMMENT 'Raw TMDB TV shows: one row per show, endpoint and batch';
