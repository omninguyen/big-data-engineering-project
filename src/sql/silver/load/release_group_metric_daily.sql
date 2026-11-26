-- Incremental load of one bronze batch into silver.release_group_metric_daily (pipeline.py silver), run once per new
-- batch, oldest first. A new day adds rows; a later batch of the same day replaces that day's values.
-- Albums without ListenBrainz data (NULL counts) are left out.
MERGE INTO ${catalog}.silver.release_group_metric_daily AS t
USING (
  SELECT
    release_group_mbid         AS release_group_id,
    dt                         AS snapshot_date,
    max(total_listen_count)    AS listen_count,
    max(total_user_count)      AS listener_count,
    max(batch_id)              AS batch_id
  FROM ${catalog}.bronze.listenbrainz_release_group_popularity
  WHERE batch_id = '${batch_id}' AND release_group_mbid <> '' AND total_listen_count IS NOT NULL AND dt IS NOT NULL
  GROUP BY release_group_mbid, dt
) AS s
ON t.release_group_id = s.release_group_id AND t.snapshot_date = s.snapshot_date
WHEN MATCHED AND s.batch_id > t.batch_id THEN UPDATE SET
  listen_count = s.listen_count, listener_count = s.listener_count, batch_id = s.batch_id,
  _loaded_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT
  (release_group_id, snapshot_date, listen_count, listener_count, batch_id, _loaded_at)
  VALUES (s.release_group_id, s.snapshot_date, s.listen_count, s.listener_count, s.batch_id, current_timestamp());
