-- Gold fact of catalog changes (a factless event fact): one row per version of a movie or album in silver (SCD2),
-- 'new' for its first version, 'changed' for each later one (its title, release date, ... changed).
-- Created if missing by: pipeline.py setup, after dim_date and dim_media (its foreign keys).
-- Rebuilt on every run from silver (gold/load/fact_media_change.sql).
CREATE TABLE IF NOT EXISTS ${catalog}.gold.fact_media_change (
  date_key     INT        NOT NULL COMMENT 'day of the change in Vietnam time, yyyymmdd (gold.dim_date)',
  media_key    STRING     NOT NULL COMMENT 'gold.dim_media',
  change_type  STRING     NOT NULL COMMENT 'new: first seen by the pipeline | changed: a new version of its details',
  changed_at   TIMESTAMP  NOT NULL COMMENT 'valid_from of the version in silver: when the ingest fetched it',
  CONSTRAINT fact_media_change_pk PRIMARY KEY (media_key, changed_at),
  CONSTRAINT fact_media_change_date_fk FOREIGN KEY (date_key) REFERENCES ${catalog}.gold.dim_date,
  CONSTRAINT fact_media_change_media_fk FOREIGN KEY (media_key) REFERENCES ${catalog}.gold.dim_media
)
USING DELTA
LOCATION '${gold_root}/fact_media_change'
COMMENT 'New movies and albums, and changes of their details (SCD2 versions in silver), full load';
