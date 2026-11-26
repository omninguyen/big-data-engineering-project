-- Bronze table of the raw TMDB files (landing/tmdb/movies/). Created if missing by: pipeline.py bronze
CREATE TABLE IF NOT EXISTS ${catalog}.bronze.tmdb_movies (
  id                 STRING         COMMENT 'TMDB id (sample:<n> for the offline sample set)',
  title              STRING,
  original_title     STRING,
  overview           STRING,
  release_date       STRING         COMMENT 'as delivered (YYYY-MM-DD)',
  genres             ARRAY<STRING>  COMMENT 'genre names, mapped from genre_ids at ingest',
  genre_ids          ARRAY<BIGINT>,
  popularity         DOUBLE,
  vote_average       DOUBLE,
  vote_count         BIGINT,
  original_language  STRING,
  poster_path        STRING,
  adult              BOOLEAN,
  endpoint           STRING         COMMENT 'movie/popular or movie/top_rated',
  dt                 DATE           COMMENT 'ingest date, from the dt= folder',
  batch_id           STRING         COMMENT 'ingest run, from the batch_id= folder',
  _source_file       STRING         COMMENT 'path of the delivered file',
  _ingested_at       TIMESTAMP      COMMENT 'when the API was called (from the file)',
  _file_size         BIGINT         COMMENT 'size in bytes of the delivered file',
  _file_modified_at  TIMESTAMP      COMMENT 'when the delivered file was last written'
)
USING DELTA
LOCATION '${tables_root}/tmdb_movies'
COMMENT 'Raw TMDB movies: one row per movie, endpoint and batch';
