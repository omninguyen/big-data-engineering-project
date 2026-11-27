-- Incremental load of one bronze batch into silver.tv_show_metric_daily (pipeline.py silver), run once per new
-- batch, oldest first. A new day adds rows; a later batch of the same day replaces that day's values.
MERGE INTO ${catalog}.silver.tv_show_metric_daily AS t
USING (
  SELECT
    try_cast(id AS BIGINT)            AS tv_id,
    dt                                AS snapshot_date,
    max(popularity)                   AS popularity,
    max(vote_average)                 AS vote_average,
    try_cast(max(vote_count) AS INT)  AS vote_count,
    max(batch_id)                     AS batch_id
  FROM ${catalog}.bronze.tmdb_tv_shows
  WHERE batch_id = '${batch_id}' AND try_cast(id AS BIGINT) IS NOT NULL AND dt IS NOT NULL
  GROUP BY try_cast(id AS BIGINT), dt
) AS s
ON t.tv_id = s.tv_id AND t.snapshot_date = s.snapshot_date
WHEN MATCHED AND s.batch_id > t.batch_id THEN UPDATE SET
  popularity = s.popularity, vote_average = s.vote_average, vote_count = s.vote_count, batch_id = s.batch_id,
  _loaded_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT
  (tv_id, snapshot_date, popularity, vote_average, vote_count, batch_id, _loaded_at)
  VALUES (s.tv_id, s.snapshot_date, s.popularity, s.vote_average, s.vote_count, s.batch_id, current_timestamp());
