-- Full load of silver.genre (pipeline.py silver): rebuilt on every run, in one write (INSERT OVERWRITE), from the
-- bronze tables: the genres of TMDB movies (sample movies left out, as in silver.movie) and the genres the
-- MusicBrainz ingest searches for (query_genre, see silver/load/release_group_genre.sql).
INSERT OVERWRITE ${catalog}.silver.genre (genre_id, name, source, _loaded_at)
SELECT
  genre_id,
  min(name)            AS name,
  min(source)          AS source,
  current_timestamp()  AS _loaded_at
FROM (
  SELECT concat('tmdb:', lower(trim(name))) AS genre_id, trim(name) AS name, 'tmdb' AS source
  FROM (
    SELECT explode(genres) AS name
    FROM ${catalog}.bronze.tmdb_movies
    WHERE try_cast(id AS BIGINT) IS NOT NULL
  )
  UNION ALL
  SELECT concat('musicbrainz:', lower(trim(query_genre))), trim(query_genre), 'musicbrainz'
  FROM ${catalog}.bronze.musicbrainz_release_groups
)
WHERE trim(name) <> ''
GROUP BY genre_id;
