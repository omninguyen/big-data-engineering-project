-- Bronze table of the raw MusicBrainz files (landing/musicbrainz/release_groups/). Created if missing by:
-- pipeline.py bronze
CREATE TABLE IF NOT EXISTS ${catalog}.bronze.musicbrainz_release_groups (
  id                  STRING         COMMENT 'MusicBrainz release group id (UUID)',
  title               STRING,
  first_release_date  STRING         COMMENT 'as delivered: YYYY, YYYY-MM or YYYY-MM-DD',
  primary_type        STRING         COMMENT 'Album, Single, EP, ...',
  secondary_types     ARRAY<STRING>  COMMENT 'e.g. Live, Compilation',
  artist_credit       ARRAY<STRUCT<name: STRING, joinphrase: STRING, artist: STRUCT<id: STRING, name: STRING>>>,
  tags                ARRAY<STRUCT<count: INT, name: STRING>>  COMMENT 'genre tags with their vote counts',
  query_genre         STRING         COMMENT 'genre of the search query the record came from',
  score               INT            COMMENT 'search relevance 0-100',
  dt                  DATE           COMMENT 'ingest date, from the dt= folder',
  batch_id            STRING         COMMENT 'ingest run, from the batch_id= folder',
  _source_file        STRING         COMMENT 'path of the delivered file',
  _ingested_at        TIMESTAMP      COMMENT 'when the API was called (from the file)',
  _file_size          BIGINT         COMMENT 'size in bytes of the delivered file',
  _file_modified_at   TIMESTAMP      COMMENT 'when the delivered file was last written'
)
USING DELTA
LOCATION '${tables_root}/musicbrainz_release_groups'
COMMENT 'Raw MusicBrainz release groups: one row per album, search genre and batch';
