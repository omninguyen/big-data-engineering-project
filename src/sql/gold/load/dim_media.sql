-- Full load of gold.dim_media (pipeline.py gold): rebuilt on every run, in one write (INSERT OVERWRITE), from the
-- current version of each movie, TV show and album in silver, with their genres.
WITH media AS (
  SELECT
    concat('movie:', CAST(movie_id AS STRING))  AS media_key,
    CAST(movie_id AS STRING)                    AS media_id,
    'movie'                                     AS media_type,
    title,
    CAST(NULL AS STRING)                        AS creator,
    release_date,
    year(release_date)                          AS release_year,
    original_language                           AS language,
    'tmdb'                                      AS source
  FROM ${catalog}.silver.movie
  WHERE is_current
  UNION ALL
  SELECT
    concat('tv:', CAST(tv_id AS STRING)), CAST(tv_id AS STRING), 'tv', name, CAST(NULL AS STRING), first_air_date,
    year(first_air_date), original_language, 'tmdb'
  FROM ${catalog}.silver.tv_show
  WHERE is_current
  UNION ALL
  SELECT
    concat('music:', release_group_id), release_group_id, 'music', title, artist_credit_name, release_date,
    release_year, CAST(NULL AS STRING), 'musicbrainz'
  FROM ${catalog}.silver.release_group
  WHERE is_current
),
media_genre AS (
  SELECT concat('movie:', CAST(mg.movie_id AS STRING)) AS media_key, mg.position, g.name
  FROM ${catalog}.silver.movie_genre AS mg
  JOIN ${catalog}.silver.genre AS g ON g.genre_id = mg.genre_id
  UNION ALL
  SELECT concat('tv:', CAST(tg.tv_id AS STRING)), tg.position, g.name
  FROM ${catalog}.silver.tv_show_genre AS tg
  JOIN ${catalog}.silver.genre AS g ON g.genre_id = tg.genre_id
  UNION ALL
  SELECT concat('music:', rg.release_group_id), rg.position, g.name
  FROM ${catalog}.silver.release_group_genre AS rg
  JOIN ${catalog}.silver.genre AS g ON g.genre_id = rg.genre_id
),
-- the genre names of each media in order (position 0 is the primary genre); a media without genres gets NULL
genres AS (
  SELECT media_key, transform(sort_array(collect_list(struct(position, name))), x -> x.name) AS names
  FROM media_genre
  GROUP BY media_key
)
INSERT OVERWRITE ${catalog}.gold.dim_media
  (media_key, media_id, media_type, title, creator, release_date, release_year, release_decade, primary_genre, genres,
   language, source)
SELECT
  m.media_key, m.media_id, m.media_type, m.title, m.creator, m.release_date, m.release_year,
  CAST((m.release_year DIV 10) * 10 AS INT)  AS release_decade,
  g.names[0]                                 AS primary_genre,
  array_join(g.names, ', ')                  AS genres,
  m.language,
  m.source
FROM media AS m
LEFT JOIN genres AS g ON g.media_key = m.media_key;
