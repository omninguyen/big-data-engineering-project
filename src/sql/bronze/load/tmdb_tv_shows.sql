-- Loads new files of landing/tmdb/tv_shows/ into the bronze table (pipeline.py bronze).
-- COPY INTO remembers the files it loaded, so a rerun never loads a file twice.
-- Each JSONL line is read as text and its fields are picked out by name: a missing field gives NULL instead of
-- failing the load, and a value of the wrong type gives NULL (try_cast) instead of failing the whole run.
COPY INTO ${catalog}.bronze.tmdb_tv_shows
FROM (
  SELECT
    value:id                                                          AS id,
    value:name                                                        AS name,
    value:original_name                                               AS original_name,
    value:overview                                                    AS overview,
    value:first_air_date                                              AS first_air_date,
    from_json(value:genres, 'ARRAY<STRING>')                          AS genres,
    from_json(value:genre_ids, 'ARRAY<BIGINT>')                       AS genre_ids,
    try_cast(value:popularity AS DOUBLE)                              AS popularity,
    try_cast(value:vote_average AS DOUBLE)                            AS vote_average,
    try_cast(value:vote_count AS BIGINT)                              AS vote_count,
    value:original_language                                           AS original_language,
    from_json(value:origin_country, 'ARRAY<STRING>')                  AS origin_country,
    value:poster_path                                                 AS poster_path,
    try_cast(value:adult AS BOOLEAN)                                  AS adult,
    value:['_endpoint']                                               AS endpoint,
    try_cast(regexp_extract(_metadata.file_path, '/dt=([^/]+)/', 1) AS DATE)  AS dt,
    regexp_extract(_metadata.file_path, '/batch_id=([^/]+)/', 1)      AS batch_id,
    _metadata.file_path                                               AS _source_file,
    try_cast(value:['_ingested_at'] AS TIMESTAMP)                     AS _ingested_at,
    _metadata.file_size                                               AS _file_size,
    _metadata.file_modification_time                                  AS _file_modified_at
  FROM '${landing_root}/tmdb/tv_shows/'
)
FILEFORMAT = TEXT;
