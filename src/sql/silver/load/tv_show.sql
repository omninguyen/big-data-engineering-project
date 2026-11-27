-- SCD Type 2 load of one bronze batch into silver.tv_show (pipeline.py silver), run once per new batch, oldest first,
-- as silver/load/movie.sql. Rerunning a batch changes nothing: unchanged shows keep their current version.

-- 1) The latest row of each show in the batch, with the checksum of its descriptive columns.
CREATE OR REPLACE TEMP VIEW tv_show_batch AS
SELECT
  tv_id, name, original_name, overview, first_air_date, original_language, origin_country, poster_path, adult,
  sha2(to_json(struct(name, original_name, overview, first_air_date, original_language, origin_country, poster_path,
                      adult)), 256)
    AS checksum,
  valid_from, batch_id
FROM (
  SELECT
    try_cast(id AS BIGINT)                        AS tv_id,
    name, original_name, overview,
    try_cast(nullif(first_air_date, '') AS DATE)  AS first_air_date,
    original_language,
    array_join(origin_country, ',')               AS origin_country,
    poster_path, adult,
    coalesce(_ingested_at, _file_modified_at)     AS valid_from,
    batch_id,
    row_number() OVER (PARTITION BY id ORDER BY _ingested_at DESC, endpoint) AS rn
  FROM ${catalog}.bronze.tmdb_tv_shows
  WHERE batch_id = '${batch_id}' AND try_cast(id AS BIGINT) IS NOT NULL
)
WHERE rn = 1;

-- 2) Close the current version of every show whose checksum changed.
MERGE INTO ${catalog}.silver.tv_show AS t
USING tv_show_batch AS s
ON t.tv_id = s.tv_id AND t.is_current
WHEN MATCHED AND t.checksum <> s.checksum AND s.valid_from > t.valid_from THEN UPDATE SET
  valid_to = s.valid_from, is_current = false, _loaded_at = current_timestamp();

-- 3) Add a version for every show without a current one: new shows and the ones closed in step 2.
INSERT INTO ${catalog}.silver.tv_show
  (tv_id, name, original_name, overview, first_air_date, original_language, origin_country, poster_path, adult,
   checksum, valid_from, valid_to, is_current, batch_id, _loaded_at)
SELECT
  s.tv_id, s.name, s.original_name, s.overview, s.first_air_date, s.original_language, s.origin_country,
  s.poster_path, s.adult, s.checksum, s.valid_from, NULL, true, s.batch_id, current_timestamp()
FROM tv_show_batch AS s
LEFT ANTI JOIN ${catalog}.silver.tv_show AS t ON t.tv_id = s.tv_id AND t.is_current;
