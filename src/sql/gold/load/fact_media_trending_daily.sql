-- Full load of gold.fact_media_trending_daily (pipeline.py gold): rebuilt on every run, in one write
-- (INSERT OVERWRITE), from silver.movie_trending_daily. A day keeps the ranking of its latest batch only: a movie
-- that left trending in a later run of the same day is dropped. media_key and date_key are built as in
-- gold/load/fact_media_metric_daily.sql, and only media of gold.dim_media are kept.
INSERT OVERWRITE ${catalog}.gold.fact_media_trending_daily (date_key, media_key, rank)
SELECT date_key, media_key, rank
FROM (
  SELECT
    CAST(date_format(snapshot_date, 'yyyyMMdd') AS INT)  AS date_key,
    concat('movie:', CAST(movie_id AS STRING))           AS media_key,
    rank,
    batch_id,
    max(batch_id) OVER (PARTITION BY snapshot_date)      AS day_batch_id
  FROM ${catalog}.silver.movie_trending_daily
)
WHERE batch_id = day_batch_id
  AND media_key IN (SELECT media_key FROM ${catalog}.gold.dim_media);
