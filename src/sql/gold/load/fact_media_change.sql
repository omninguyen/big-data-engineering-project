-- Full load of gold.fact_media_change (pipeline.py gold): rebuilt on every run, in one write (INSERT OVERWRITE), from
-- every version in silver.movie and silver.release_group. media_key is built as in gold/load/dim_media.sql. The day
-- is the Vietnam date of valid_from, as dt and the batch id. The first run counts the whole catalog as new.
WITH versions AS (
  SELECT concat('movie:', CAST(movie_id AS STRING)) AS media_key, valid_from
  FROM ${catalog}.silver.movie
  UNION ALL
  SELECT concat('music:', release_group_id), valid_from
  FROM ${catalog}.silver.release_group
)
INSERT OVERWRITE ${catalog}.gold.fact_media_change (date_key, media_key, change_type, changed_at)
SELECT
  CAST(date_format(from_utc_timestamp(valid_from, 'Asia/Ho_Chi_Minh'), 'yyyyMMdd') AS INT)  AS date_key,
  media_key,
  CASE WHEN row_number() OVER (PARTITION BY media_key ORDER BY valid_from) = 1 THEN 'new' ELSE 'changed' END
                                                                                            AS change_type,
  valid_from                                                                                AS changed_at
FROM versions;
