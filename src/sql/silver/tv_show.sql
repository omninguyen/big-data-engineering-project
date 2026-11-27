-- Silver table of TMDB TV shows, SCD Type 2: one row per version of a show's descriptive columns.
-- Created if missing by: pipeline.py setup. Loaded batch by batch from bronze.tmdb_tv_shows
-- (silver/load/tv_show.sql).
CREATE TABLE IF NOT EXISTS ${catalog}.silver.tv_show (
  tv_id              BIGINT     NOT NULL COMMENT 'TMDB TV id',
  name               STRING,
  original_name      STRING,
  overview           STRING,
  first_air_date     DATE                COMMENT 'first episode',
  original_language  STRING              COMMENT 'ISO 639-1',
  origin_country     STRING              COMMENT 'ISO 3166-1 codes, comma separated, e.g. US or KR',
  poster_path        STRING,
  adult              BOOLEAN,
  checksum           STRING     NOT NULL COMMENT 'sha2-256 of the descriptive columns: a new value starts a version',
  valid_from         TIMESTAMP  NOT NULL COMMENT 'SCD2: when the API first returned this version (bronze _ingested_at)',
  valid_to           TIMESTAMP           COMMENT 'SCD2: end of this version (exclusive), NULL = current',
  is_current         BOOLEAN    NOT NULL,
  batch_id           STRING     NOT NULL COMMENT 'bronze batch that brought this version',
  _loaded_at         TIMESTAMP  NOT NULL COMMENT 'when this row was last written to silver',
  CONSTRAINT tv_show_pk PRIMARY KEY (tv_id, valid_from)
)
USING DELTA
LOCATION '${silver_root}/tv_show'
COMMENT 'TMDB TV shows (SCD Type 2), from bronze.tmdb_tv_shows';
