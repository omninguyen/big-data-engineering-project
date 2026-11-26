-- Silver table linking albums to their credited artists, Type 1 (current state only), from each album's latest
-- MusicBrainz batch. Created if missing by: pipeline.py silver. Rebuilt on every run
-- (silver/load/release_group_artist.sql).
CREATE TABLE IF NOT EXISTS ${catalog}.silver.release_group_artist (
  release_group_id  STRING     NOT NULL COMMENT 'MusicBrainz release group id (silver.release_group)',
  artist_id         STRING     NOT NULL COMMENT 'MusicBrainz artist id (silver.artist)',
  position          INT        NOT NULL COMMENT 'order in the artist credit, 0 = main artist',
  credited_name     STRING              COMMENT 'name as credited on the album, e.g. a former name of the artist',
  join_phrase       STRING     NOT NULL COMMENT 'text after this artist in the credit, e.g. " feat. ", " & "',
  _loaded_at        TIMESTAMP  NOT NULL COMMENT 'when this row was last written to silver',
  CONSTRAINT release_group_artist_pk PRIMARY KEY (release_group_id, artist_id)
)
USING DELTA
LOCATION '${silver_root}/release_group_artist'
COMMENT 'Credited artists of each MusicBrainz release group, full load';
