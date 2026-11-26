-- Silver table of MusicBrainz artists, SCD Type 2: one row per version of an artist's name.
-- Created if missing by: pipeline.py setup. Loaded batch by batch from the artist credits of
-- bronze.musicbrainz_release_groups (silver/load/artist.sql).
CREATE TABLE IF NOT EXISTS ${catalog}.silver.artist (
  artist_id   STRING     NOT NULL COMMENT 'MusicBrainz artist id (UUID)',
  name        STRING              COMMENT 'name of the artist (the name on an album may differ: release_group_artist)',
  checksum    STRING     NOT NULL COMMENT 'sha2-256 of the name; a change starts a version',
  valid_from  TIMESTAMP  NOT NULL COMMENT 'SCD2: when the API first returned it (bronze _ingested_at)',
  valid_to    TIMESTAMP           COMMENT 'SCD2: end of this version (exclusive), NULL = current',
  is_current  BOOLEAN    NOT NULL,
  batch_id    STRING     NOT NULL COMMENT 'bronze batch that brought this version',
  _loaded_at  TIMESTAMP  NOT NULL COMMENT 'when this row was last written to silver',
  CONSTRAINT artist_pk PRIMARY KEY (artist_id, valid_from)
)
USING DELTA
LOCATION '${silver_root}/artist'
COMMENT 'MusicBrainz artists (SCD Type 2), from the artist credits of bronze.musicbrainz_release_groups';
