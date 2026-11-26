-- Incremental load of one bronze batch into silver.movie_metric_daily (pipeline.py silver), run once per new batch,
-- oldest first. A new day adds rows; a later batch of the same day replaces that day's values.
MERGE INTO ${catalog}.silver.movie_metric_daily AS t
USING (
  SELECT
    try_cast(id AS BIGINT)                                AS movie_id,
    dt                                                    AS snapshot_date,
    max(popularity)                                       AS popularity,
    max(vote_average)                                     AS vote_average,
    try_cast(max(vote_count) AS INT)                      AS vote_count,
    coalesce(bool_or(endpoint = 'movie/popular'), false)  AS in_popular,
    coalesce(bool_or(endpoint = 'movie/top_rated'), false) AS in_top_rated,
    max(batch_id)                                         AS batch_id
  FROM ${catalog}.bronze.tmdb_movies
  WHERE batch_id = '${batch_id}' AND try_cast(id AS BIGINT) IS NOT NULL AND dt IS NOT NULL
  GROUP BY try_cast(id AS BIGINT), dt
) AS s
ON t.movie_id = s.movie_id AND t.snapshot_date = s.snapshot_date
WHEN MATCHED AND s.batch_id > t.batch_id THEN UPDATE SET
  popularity = s.popularity, vote_average = s.vote_average, vote_count = s.vote_count,
  in_popular = s.in_popular, in_top_rated = s.in_top_rated, batch_id = s.batch_id, _loaded_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT
  (movie_id, snapshot_date, popularity, vote_average, vote_count, in_popular, in_top_rated, batch_id, _loaded_at)
  VALUES (s.movie_id, s.snapshot_date, s.popularity, s.vote_average, s.vote_count, s.in_popular, s.in_top_rated,
          s.batch_id, current_timestamp());
