-- Full load of gold.fact_media_metric_daily (pipeline.py gold): rebuilt on every run, in one write
-- (INSERT OVERWRITE), from silver.movie_metric_daily, tv_show_metric_daily and release_group_metric_daily. media_key
-- is built as in gold/load/dim_media.sql, and only media found in gold.dim_media are kept, so every row has its
-- dimension.
-- listen_count is an all-time total: listen_count_change is the difference with the album's previous snapshot
-- (one day, or more when a day was missed).
WITH metric AS (
  SELECT
    snapshot_date,
    concat('movie:', CAST(movie_id AS STRING))  AS media_key,
    popularity,
    vote_average                                AS rating,
    vote_count                                  AS votes,
    CAST(NULL AS BIGINT)                        AS listen_count,
    CAST(NULL AS BIGINT)                        AS listener_count,
    CAST(NULL AS BIGINT)                        AS listen_count_change
  FROM ${catalog}.silver.movie_metric_daily
  UNION ALL
  SELECT
    snapshot_date,
    concat('tv:', CAST(tv_id AS STRING)),
    popularity,
    vote_average,
    vote_count,
    CAST(NULL AS BIGINT),
    CAST(NULL AS BIGINT),
    CAST(NULL AS BIGINT)
  FROM ${catalog}.silver.tv_show_metric_daily
  UNION ALL
  SELECT
    snapshot_date,
    concat('music:', release_group_id),
    CAST(NULL AS DOUBLE),
    CAST(NULL AS DOUBLE),
    CAST(NULL AS INT),
    listen_count,
    listener_count,
    listen_count - lag(listen_count) OVER (PARTITION BY release_group_id ORDER BY snapshot_date)
  FROM ${catalog}.silver.release_group_metric_daily
)
INSERT OVERWRITE ${catalog}.gold.fact_media_metric_daily
  (date_key, media_key, popularity, rating, votes, listen_count, listener_count, listen_count_change)
SELECT
  CAST(date_format(snapshot_date, 'yyyyMMdd') AS INT)  AS date_key,
  media_key, popularity, rating, votes, listen_count, listener_count, listen_count_change
FROM metric
WHERE media_key IN (SELECT media_key FROM ${catalog}.gold.dim_media);
