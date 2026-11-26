-- SCD Type 2 load of one bronze batch into silver.movie (pipeline.py silver), run once per new batch, oldest first.
-- Rerunning a batch changes nothing: unchanged movies keep their current version.

-- 1) The latest row of each movie in the batch, with the checksum of its descriptive columns.
--    Sample movies (id sample:<n>) are left out: silver holds real TMDB data only.
CREATE OR REPLACE TEMP VIEW movie_batch AS
SELECT
  movie_id, title, original_title, overview, release_date, original_language, poster_path, adult,
  sha2(to_json(struct(title, original_title, overview, release_date, original_language, poster_path, adult)), 256)
    AS checksum,
  valid_from, batch_id
FROM (
  SELECT
    try_cast(id AS BIGINT)                        AS movie_id,
    title, original_title, overview,
    try_cast(nullif(release_date, '') AS DATE)    AS release_date,
    original_language, poster_path, adult,
    coalesce(_ingested_at, _file_modified_at)     AS valid_from,
    batch_id,
    row_number() OVER (PARTITION BY id ORDER BY _ingested_at DESC, endpoint) AS rn
  FROM ${catalog}.bronze.tmdb_movies
  WHERE batch_id = '${batch_id}' AND try_cast(id AS BIGINT) IS NOT NULL
)
WHERE rn = 1;

-- 2) Close the current version of every movie whose checksum changed.
MERGE INTO ${catalog}.silver.movie AS t
USING movie_batch AS s
ON t.movie_id = s.movie_id AND t.is_current
WHEN MATCHED AND t.checksum <> s.checksum AND s.valid_from > t.valid_from THEN UPDATE SET
  valid_to = s.valid_from, is_current = false, _loaded_at = current_timestamp();

-- 3) Add a version for every movie without a current one: new movies and the ones closed in step 2.
INSERT INTO ${catalog}.silver.movie
  (movie_id, title, original_title, overview, release_date, original_language, poster_path, adult,
   checksum, valid_from, valid_to, is_current, batch_id, _loaded_at)
SELECT
  s.movie_id, s.title, s.original_title, s.overview, s.release_date, s.original_language, s.poster_path, s.adult,
  s.checksum, s.valid_from, NULL, true, s.batch_id, current_timestamp()
FROM movie_batch AS s
LEFT ANTI JOIN ${catalog}.silver.movie AS t ON t.movie_id = s.movie_id AND t.is_current;
