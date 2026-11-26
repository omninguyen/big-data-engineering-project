-- Silver table of daily ListenBrainz metrics per album: one row per release group and snapshot date.
-- Created if missing by: pipeline.py setup. Loaded batch by batch from bronze.listenbrainz_release_group_popularity
-- (silver/load/release_group_metric_daily.sql). The counts are all-time totals: the listens of a day are the
-- difference with the day before (gold).
CREATE TABLE IF NOT EXISTS ${catalog}.silver.release_group_metric_daily (
  release_group_id  STRING     NOT NULL COMMENT 'MusicBrainz release group id (silver.release_group)',
  snapshot_date     DATE       NOT NULL COMMENT 'ingest date (bronze dt)',
  listen_count      BIGINT     NOT NULL COMMENT 'all-time listens on ListenBrainz, as of that day',
  listener_count    BIGINT              COMMENT 'all-time distinct listeners on ListenBrainz, as of that day',
  batch_id          STRING     NOT NULL COMMENT 'bronze batch of these values: the latest of the day',
  _loaded_at        TIMESTAMP  NOT NULL COMMENT 'when this row was last written to silver',
  CONSTRAINT release_group_metric_daily_pk PRIMARY KEY (release_group_id, snapshot_date)
)
USING DELTA
LOCATION '${silver_root}/release_group_metric_daily'
COMMENT 'Daily ListenBrainz listen counts per album (incremental), from bronze.listenbrainz_release_group_popularity';
