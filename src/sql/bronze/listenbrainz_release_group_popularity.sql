-- Bronze table of the raw ListenBrainz files (landing/listenbrainz/release_group_popularity/). Created if missing
-- by: pipeline.py setup
CREATE TABLE IF NOT EXISTS ${catalog}.bronze.listenbrainz_release_group_popularity (
  release_group_mbid  STRING     COMMENT 'MusicBrainz release group id (UUID) that was looked up',
  total_listen_count  BIGINT     COMMENT 'all-time listens on ListenBrainz; NULL = no data for the album',
  total_user_count    BIGINT     COMMENT 'all-time distinct listeners on ListenBrainz; NULL = no data',
  dt                  DATE       COMMENT 'ingest date, from the dt= folder',
  batch_id            STRING     COMMENT 'ingest run, from the batch_id= folder',
  _source_file        STRING     COMMENT 'path of the delivered file',
  _ingested_at        TIMESTAMP  COMMENT 'when the API was called (from the file)',
  _file_size          BIGINT     COMMENT 'size in bytes of the delivered file',
  _file_modified_at   TIMESTAMP  COMMENT 'when the delivered file was last written'
)
USING DELTA
LOCATION '${tables_root}/listenbrainz_release_group_popularity'
COMMENT 'Raw ListenBrainz popularity: one row per album and batch';
