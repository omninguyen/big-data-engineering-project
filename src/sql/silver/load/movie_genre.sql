-- Full load of silver.movie_genre (pipeline.py silver): rebuilt on every run, in one write (INSERT OVERWRITE), from
-- the latest bronze row of each movie in silver.movie.
INSERT OVERWRITE ${catalog}.silver.movie_genre (movie_id, genre_id, position, _loaded_at)
SELECT
  movie_id,
  genre_id,
  min(position)        AS position,
  current_timestamp()  AS _loaded_at
FROM (
  SELECT movie_id, concat('tmdb:', lower(trim(name))) AS genre_id, position
  FROM (
    SELECT movie_id, posexplode(genres) AS (position, name)
    FROM (
      SELECT
        try_cast(id AS BIGINT) AS movie_id,
        genres,
        row_number() OVER (PARTITION BY try_cast(id AS BIGINT) ORDER BY batch_id DESC, _ingested_at DESC) AS rn
      FROM ${catalog}.bronze.tmdb_movies
      WHERE try_cast(id AS BIGINT) IS NOT NULL
    )
    WHERE rn = 1
  )
  WHERE trim(name) <> ''
)
WHERE movie_id IN (SELECT movie_id FROM ${catalog}.silver.movie)
GROUP BY movie_id, genre_id;
