-- Full load of gold.dim_genre (pipeline.py gold): rebuilt on every run, in one write (INSERT OVERWRITE), from
-- silver.genre. A genre of an unknown source has no media_type and fails the load (NOT NULL), on purpose.
INSERT OVERWRITE ${catalog}.gold.dim_genre (genre_id, genre_name, media_type)
SELECT
  genre_id,
  name                                                                                               AS genre_name,
  CASE source WHEN 'tmdb' THEN 'movie' WHEN 'tmdb_tv' THEN 'tv' WHEN 'musicbrainz' THEN 'music' END  AS media_type
FROM ${catalog}.silver.genre;
