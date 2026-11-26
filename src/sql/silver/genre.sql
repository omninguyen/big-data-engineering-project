-- Silver table of genres, Type 1 (current state only): one row per genre of each source.
-- Created if missing by: pipeline.py silver. Rebuilt on every run from the bronze tables (silver/load/genre.sql).
CREATE TABLE IF NOT EXISTS ${catalog}.silver.genre (
  genre_id    STRING     NOT NULL COMMENT '<source>:<lower-case name>, e.g. tmdb:science fiction, musicbrainz:rock',
  name        STRING     NOT NULL COMMENT 'display name as delivered',
  source      STRING     NOT NULL COMMENT 'tmdb | musicbrainz',
  _loaded_at  TIMESTAMP  NOT NULL COMMENT 'when this row was last written to silver',
  CONSTRAINT genre_pk PRIMARY KEY (genre_id)
)
USING DELTA
LOCATION '${silver_root}/genre'
COMMENT 'Genres of TMDB movies (and, later, of MusicBrainz albums), full load';
