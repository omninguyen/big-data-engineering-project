-- Full load of silver.release_group_genre (pipeline.py silver): rebuilt on every run, in one write
-- (INSERT OVERWRITE), from the latest bronze batch of each album in silver.release_group.
-- MusicBrainz tags mix genres with other words ("british", "seen live"), so only the genres the ingest searches for
-- (MB_GENRES, seen in bronze as query_genre) are kept: the genre an album was found by, and its tags among them.
WITH latest AS (
  SELECT id AS release_group_id, lower(trim(query_genre)) AS query_genre, tags
  FROM (
    SELECT *, max(batch_id) OVER (PARTITION BY id) AS latest_batch_id
    FROM ${catalog}.bronze.musicbrainz_release_groups
    WHERE id <> ''
  )
  WHERE batch_id = latest_batch_id
),
genres AS (
  SELECT DISTINCT lower(trim(query_genre)) AS genre
  FROM ${catalog}.bronze.musicbrainz_release_groups
  WHERE trim(query_genre) <> ''
),
found AS (
  SELECT release_group_id, query_genre AS genre, 0 AS tag_count
  FROM latest
  UNION ALL
  SELECT release_group_id, lower(trim(tag.name)) AS genre, coalesce(tag.`count`, 0) AS tag_count
  FROM (SELECT release_group_id, explode(tags) AS tag FROM latest)
),
counted AS (
  SELECT f.release_group_id, f.genre, max(f.tag_count) AS tag_count
  FROM found AS f
  JOIN genres AS g ON f.genre = g.genre
  WHERE f.release_group_id IN (SELECT release_group_id FROM ${catalog}.silver.release_group)
  GROUP BY f.release_group_id, f.genre
)
INSERT OVERWRITE ${catalog}.silver.release_group_genre (release_group_id, genre_id, position, tag_count, _loaded_at)
SELECT
  release_group_id,
  concat('musicbrainz:', genre)                                                                AS genre_id,
  CAST(row_number() OVER (PARTITION BY release_group_id ORDER BY tag_count DESC, genre) - 1 AS INT)  AS position,
  CAST(tag_count AS INT)                                                                       AS tag_count,
  current_timestamp()                                                                          AS _loaded_at
FROM counted;
