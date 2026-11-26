-- SCD Type 2 load of one bronze batch into silver.release_group (pipeline.py silver), run once per new batch,
-- oldest first. Rerunning a batch changes nothing: unchanged albums keep their current version.

-- 1) One row per album in the batch (an album found by several genre searches has a row per search), with the
--    checksum of its descriptive columns. Tags are left out of the checksum: their vote counts change almost daily.
CREATE OR REPLACE TEMP VIEW release_group_batch AS
SELECT
  release_group_id, title, artist_credit_name, first_release_date,
  CASE WHEN length(first_release_date) = 10 THEN try_cast(first_release_date AS DATE) END  AS release_date,
  try_cast(left(first_release_date, 4) AS INT)                                             AS release_year,
  primary_type, secondary_types,
  sha2(to_json(struct(title, artist_credit_name, first_release_date, primary_type, secondary_types)), 256)
    AS checksum,
  valid_from, batch_id
FROM (
  SELECT
    id                                            AS release_group_id,
    title,
    array_join(transform(artist_credit, c -> concat(coalesce(c.name, c.artist.name, ''), coalesce(c.joinphrase, ''))),
               '')                                AS artist_credit_name,
    nullif(first_release_date, '')                AS first_release_date,
    primary_type,
    secondary_types,
    coalesce(_ingested_at, _file_modified_at)     AS valid_from,
    batch_id,
    row_number() OVER (PARTITION BY id ORDER BY _ingested_at DESC, query_genre) AS rn
  FROM ${catalog}.bronze.musicbrainz_release_groups
  WHERE batch_id = '${batch_id}' AND id <> ''
)
WHERE rn = 1;

-- 2) Close the current version of every album whose checksum changed.
MERGE INTO ${catalog}.silver.release_group AS t
USING release_group_batch AS s
ON t.release_group_id = s.release_group_id AND t.is_current
WHEN MATCHED AND t.checksum <> s.checksum AND s.valid_from > t.valid_from THEN UPDATE SET
  valid_to = s.valid_from, is_current = false, _loaded_at = current_timestamp();

-- 3) Add a version for every album without a current one: new albums and the ones closed in step 2.
INSERT INTO ${catalog}.silver.release_group
  (release_group_id, title, artist_credit_name, first_release_date, release_date, release_year, primary_type,
   secondary_types, checksum, valid_from, valid_to, is_current, batch_id, _loaded_at)
SELECT
  s.release_group_id, s.title, s.artist_credit_name, s.first_release_date, s.release_date, s.release_year,
  s.primary_type, s.secondary_types, s.checksum, s.valid_from, NULL, true, s.batch_id, current_timestamp()
FROM release_group_batch AS s
LEFT ANTI JOIN ${catalog}.silver.release_group AS t ON t.release_group_id = s.release_group_id AND t.is_current;
