-- Full load of silver.tv_show_genre (pipeline.py silver): rebuilt on every run, in one write (INSERT OVERWRITE), from
-- the latest bronze row of each show in silver.tv_show, as silver/load/movie_genre.sql.
INSERT OVERWRITE ${catalog}.silver.tv_show_genre (tv_id, genre_id, position, _loaded_at)
SELECT
  tv_id,
  genre_id,
  min(position)        AS position,
  current_timestamp()  AS _loaded_at
FROM (
  SELECT tv_id, concat('tmdb_tv:', lower(trim(name))) AS genre_id, position
  FROM (
    SELECT tv_id, posexplode(genres) AS (position, name)
    FROM (
      SELECT
        try_cast(id AS BIGINT) AS tv_id,
        genres,
        row_number() OVER (PARTITION BY try_cast(id AS BIGINT) ORDER BY batch_id DESC, _ingested_at DESC) AS rn
      FROM ${catalog}.bronze.tmdb_tv_shows
      WHERE try_cast(id AS BIGINT) IS NOT NULL
    )
    WHERE rn = 1
  )
  WHERE trim(name) <> ''
)
WHERE tv_id IN (SELECT tv_id FROM ${catalog}.silver.tv_show)
GROUP BY tv_id, genre_id;
