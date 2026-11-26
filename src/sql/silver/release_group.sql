-- Silver table of MusicBrainz release groups (albums), SCD Type 2: one row per version of an album's descriptive
-- columns. Created if missing by: pipeline.py setup. Loaded batch by batch from bronze.musicbrainz_release_groups
-- (silver/load/release_group.sql).
CREATE TABLE IF NOT EXISTS ${catalog}.silver.release_group (
  release_group_id    STRING         NOT NULL COMMENT 'MusicBrainz release group id (UUID)',
  title               STRING,
  artist_credit_name  STRING                  COMMENT 'artists as credited, e.g. Simon & Garfunkel',
  first_release_date  STRING                  COMMENT 'as delivered: YYYY, YYYY-MM or YYYY-MM-DD',
  release_date        DATE                    COMMENT 'first_release_date when it is a full date (YYYY-MM-DD)',
  release_year        INT,
  primary_type        STRING                  COMMENT 'Album, Single, EP, ...',
  secondary_types     ARRAY<STRING>           COMMENT 'e.g. Live, Compilation',
  checksum            STRING         NOT NULL COMMENT 'sha2-256 of the descriptive columns; a change starts a version',
  valid_from          TIMESTAMP      NOT NULL COMMENT 'SCD2: when the API first returned it (bronze _ingested_at)',
  valid_to            TIMESTAMP               COMMENT 'SCD2: end of this version (exclusive), NULL = current',
  is_current          BOOLEAN        NOT NULL,
  batch_id            STRING         NOT NULL COMMENT 'bronze batch that brought this version',
  _loaded_at          TIMESTAMP      NOT NULL COMMENT 'when this row was last written to silver',
  CONSTRAINT release_group_pk PRIMARY KEY (release_group_id, valid_from)
)
USING DELTA
LOCATION '${silver_root}/release_group'
COMMENT 'MusicBrainz release groups (SCD Type 2), from bronze.musicbrainz_release_groups';
