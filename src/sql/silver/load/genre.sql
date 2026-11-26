-- Full load of silver.genre (pipeline.py silver): rebuilt on every run, in one write (INSERT OVERWRITE), from every
-- genre name in bronze. Sample movies are left out, as in silver.movie.
INSERT OVERWRITE ${catalog}.silver.genre (genre_id, name, source, _loaded_at)
SELECT
  genre_id,
  min(name)            AS name,
  'tmdb'               AS source,
  current_timestamp()  AS _loaded_at
FROM (
  SELECT concat('tmdb:', lower(trim(name))) AS genre_id, trim(name) AS name
  FROM (
    SELECT explode(genres) AS name
    FROM ${catalog}.bronze.tmdb_movies
    WHERE try_cast(id AS BIGINT) IS NOT NULL
  )
  WHERE trim(name) <> ''
)
GROUP BY genre_id;
