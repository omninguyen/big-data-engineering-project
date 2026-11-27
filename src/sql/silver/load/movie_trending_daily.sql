-- Incremental load of one bronze batch into silver.movie_trending_daily (pipeline.py silver), run once per new batch,
-- oldest first. A new day adds rows; a later batch of the same day replaces the rank of the movies it holds again
-- (gold keeps the latest batch of each day only, see gold/load/fact_media_trending_daily.sql).
MERGE INTO ${catalog}.silver.movie_trending_daily AS t
USING (
  SELECT
    try_cast(id AS BIGINT)  AS movie_id,
    dt                      AS snapshot_date,
    min(rank)               AS rank,
    max(batch_id)           AS batch_id
  FROM ${catalog}.bronze.tmdb_trending_movies
  WHERE batch_id = '${batch_id}' AND try_cast(id AS BIGINT) IS NOT NULL AND rank IS NOT NULL AND dt IS NOT NULL
  GROUP BY try_cast(id AS BIGINT), dt
) AS s
ON t.movie_id = s.movie_id AND t.snapshot_date = s.snapshot_date
WHEN MATCHED AND s.batch_id > t.batch_id THEN UPDATE SET
  rank = s.rank, batch_id = s.batch_id, _loaded_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT
  (movie_id, snapshot_date, rank, batch_id, _loaded_at)
  VALUES (s.movie_id, s.snapshot_date, s.rank, s.batch_id, current_timestamp());
