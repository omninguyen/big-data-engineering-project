-- SCD Type 2 load of one bronze batch into silver.artist (pipeline.py silver), run once per new batch, oldest first.
-- Rerunning a batch changes nothing: unchanged artists keep their current version.

-- 1) One row per artist credited on an album of the batch. MusicBrainz credits always name the artist entity;
--    a credit without an artist id could not be linked, so it is left out.
CREATE OR REPLACE TEMP VIEW artist_batch AS
SELECT artist_id, name, sha2(to_json(struct(name)), 256) AS checksum, valid_from, batch_id
FROM (
  SELECT
    credit.artist.id                              AS artist_id,
    credit.artist.name                            AS name,
    coalesce(_ingested_at, _file_modified_at)     AS valid_from,
    batch_id,
    row_number() OVER (PARTITION BY credit.artist.id ORDER BY _ingested_at DESC, credit.artist.name) AS rn
  FROM (
    SELECT explode(artist_credit) AS credit, _ingested_at, _file_modified_at, batch_id
    FROM ${catalog}.bronze.musicbrainz_release_groups
    WHERE batch_id = '${batch_id}' AND id <> ''
  )
  WHERE credit.artist.id <> ''
)
WHERE rn = 1;

-- 2) Close the current version of every artist whose checksum changed.
MERGE INTO ${catalog}.silver.artist AS t
USING artist_batch AS s
ON t.artist_id = s.artist_id AND t.is_current
WHEN MATCHED AND t.checksum <> s.checksum AND s.valid_from > t.valid_from THEN UPDATE SET
  valid_to = s.valid_from, is_current = false, _loaded_at = current_timestamp();

-- 3) Add a version for every artist without a current one: new artists and the ones closed in step 2.
INSERT INTO ${catalog}.silver.artist
  (artist_id, name, checksum, valid_from, valid_to, is_current, batch_id, _loaded_at)
SELECT s.artist_id, s.name, s.checksum, s.valid_from, NULL, true, s.batch_id, current_timestamp()
FROM artist_batch AS s
LEFT ANTI JOIN ${catalog}.silver.artist AS t ON t.artist_id = s.artist_id AND t.is_current;
