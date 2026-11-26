-- Silver table of TMDB movies, SCD Type 2: one row per version of a movie's descriptive columns.
-- Created if missing by: pipeline.py setup. Loaded batch by batch from bronze.tmdb_movies (silver/load/movie.sql).
CREATE TABLE IF NOT EXISTS ${catalog}.silver.movie (
  movie_id           BIGINT     NOT NULL COMMENT 'TMDB id',
  title              STRING,
  original_title     STRING,
  overview           STRING,
  release_date       DATE,
  original_language  STRING              COMMENT 'ISO 639-1',
  poster_path        STRING,
  adult              BOOLEAN,
  checksum           STRING     NOT NULL COMMENT 'sha2-256 of the descriptive columns: a new value starts a version',
  valid_from         TIMESTAMP  NOT NULL COMMENT 'SCD2: when the API first returned this version (bronze _ingested_at)',
  valid_to           TIMESTAMP           COMMENT 'SCD2: end of this version (exclusive), NULL = current',
  is_current         BOOLEAN    NOT NULL,
  batch_id           STRING     NOT NULL COMMENT 'bronze batch that brought this version',
  _loaded_at         TIMESTAMP  NOT NULL COMMENT 'when this row was last written to silver',
  CONSTRAINT movie_pk PRIMARY KEY (movie_id, valid_from)
)
USING DELTA
LOCATION '${silver_root}/movie'
COMMENT 'TMDB movies (SCD Type 2), from bronze.tmdb_movies';
