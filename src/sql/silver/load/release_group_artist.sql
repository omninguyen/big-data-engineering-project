-- Full load of silver.release_group_artist (pipeline.py silver): rebuilt on every run, in one write
-- (INSERT OVERWRITE), from the artist credit of each album's latest bronze row, for albums in silver.release_group
-- and artists in silver.artist.
WITH latest AS (
  SELECT
    id AS release_group_id,
    artist_credit,
    row_number() OVER (PARTITION BY id ORDER BY batch_id DESC, _ingested_at DESC, query_genre) AS rn
  FROM ${catalog}.bronze.musicbrainz_release_groups
  WHERE id <> ''
),
credits AS (
  SELECT
    release_group_id,
    credit.artist.id                                AS artist_id,
    position,
    coalesce(credit.name, credit.artist.name)       AS credited_name,
    coalesce(credit.joinphrase, '')                 AS join_phrase
  FROM (SELECT release_group_id, posexplode(artist_credit) AS (position, credit) FROM latest WHERE rn = 1)
  WHERE credit.artist.id <> ''
)
INSERT OVERWRITE ${catalog}.silver.release_group_artist
  (release_group_id, artist_id, position, credited_name, join_phrase, _loaded_at)
SELECT
  release_group_id,
  artist_id,
  min(position)                       AS position,
  min_by(credited_name, position)     AS credited_name,
  min_by(join_phrase, position)       AS join_phrase,
  current_timestamp()                 AS _loaded_at
FROM credits
WHERE release_group_id IN (SELECT release_group_id FROM ${catalog}.silver.release_group)
  AND artist_id IN (SELECT artist_id FROM ${catalog}.silver.artist)
GROUP BY release_group_id, artist_id;
