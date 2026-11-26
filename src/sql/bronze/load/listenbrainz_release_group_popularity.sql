-- Loads new files of landing/listenbrainz/release_group_popularity/ into the bronze table (pipeline.py bronze).
-- COPY INTO remembers the files it loaded, so a rerun never loads a file twice.
-- Each JSONL line is read as text and its fields are picked out by name: a missing field gives NULL instead of
-- failing the load, and a value of the wrong type gives NULL (try_cast) instead of failing the whole run.
COPY INTO ${catalog}.bronze.listenbrainz_release_group_popularity
FROM (
  SELECT
    value:release_group_mbid                                          AS release_group_mbid,
    try_cast(value:total_listen_count AS BIGINT)                      AS total_listen_count,
    try_cast(value:total_user_count AS BIGINT)                        AS total_user_count,
    try_cast(regexp_extract(_metadata.file_path, '/dt=([^/]+)/', 1) AS DATE)  AS dt,
    regexp_extract(_metadata.file_path, '/batch_id=([^/]+)/', 1)      AS batch_id,
    _metadata.file_path                                               AS _source_file,
    try_cast(value:['_ingested_at'] AS TIMESTAMP)                     AS _ingested_at,
    _metadata.file_size                                               AS _file_size,
    _metadata.file_modification_time                                  AS _file_modified_at
  FROM '${landing_root}/listenbrainz/release_group_popularity/'
)
FILEFORMAT = TEXT;
