-- Full load of gold.bridge_media_genre (pipeline.py gold): rebuilt on every run, in one write (INSERT OVERWRITE),
-- from silver.movie_genre, tv_show_genre and release_group_genre. media_key is built as in gold/load/dim_media.sql,
-- and the primary genre is the one with the lowest position, as dim_media.primary_genre.
INSERT OVERWRITE ${catalog}.gold.bridge_media_genre (media_key, genre_id, is_primary)
SELECT
  media_key,
  genre_id,
  row_number() OVER (PARTITION BY media_key ORDER BY position) = 1  AS is_primary
FROM (
  SELECT concat('movie:', CAST(movie_id AS STRING)) AS media_key, genre_id, position
  FROM ${catalog}.silver.movie_genre
  UNION ALL
  SELECT concat('tv:', CAST(tv_id AS STRING)), genre_id, position
  FROM ${catalog}.silver.tv_show_genre
  UNION ALL
  SELECT concat('music:', release_group_id), genre_id, position
  FROM ${catalog}.silver.release_group_genre
);
