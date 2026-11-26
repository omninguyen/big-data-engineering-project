-- Loads new files of landing/musicbrainz/release_groups/ into the bronze table (pipeline.py bronze).
-- COPY INTO remembers the files it loaded, so a rerun never loads a file twice.
-- Each JSONL line is read as text and its fields are picked out by name: a missing field gives NULL instead of
-- failing the load, and a value of the wrong type gives NULL (try_cast) instead of failing the whole run.
COPY INTO ${catalog}.bronze.musicbrainz_release_groups
FROM (
  SELECT
    value:id                                                          AS id,
    value:title                                                       AS title,
    value:['first-release-date']                                      AS first_release_date,
    value:['primary-type']                                            AS primary_type,
    from_json(value:['secondary-types'], 'ARRAY<STRING>')             AS secondary_types,
    from_json(value:['artist-credit'],
              'ARRAY<STRUCT<name: STRING, joinphrase: STRING, artist: STRUCT<id: STRING, name: STRING>>>')
                                                                      AS artist_credit,
    from_json(value:tags, 'ARRAY<STRUCT<count: INT, name: STRING>>')  AS tags,
    value:['_query_genre']                                            AS query_genre,
    try_cast(value:score AS INT)                                      AS score,
    try_cast(regexp_extract(_metadata.file_path, '/dt=([^/]+)/', 1) AS DATE)  AS dt,
    regexp_extract(_metadata.file_path, '/batch_id=([^/]+)/', 1)      AS batch_id,
    _metadata.file_path                                               AS _source_file,
    try_cast(value:['_ingested_at'] AS TIMESTAMP)                     AS _ingested_at,
    _metadata.file_size                                               AS _file_size,
    _metadata.file_modification_time                                  AS _file_modified_at
  FROM '${landing_root}/musicbrainz/release_groups/'
)
FILEFORMAT = TEXT;
