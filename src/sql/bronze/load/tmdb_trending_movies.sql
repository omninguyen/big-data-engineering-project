-- Loads new files of landing/tmdb/trending_movies/ into the bronze table (pipeline.py bronze).
-- COPY INTO remembers the files it loaded, so a rerun never loads a file twice.
COPY INTO ${catalog}.bronze.tmdb_trending_movies
FROM (
  SELECT
    value:id                                                          AS id,
    try_cast(value:rank AS INT)                                       AS rank,
    try_cast(regexp_extract(_metadata.file_path, '/dt=([^/]+)/', 1) AS DATE)  AS dt,
    regexp_extract(_metadata.file_path, '/batch_id=([^/]+)/', 1)      AS batch_id,
    _metadata.file_path                                               AS _source_file,
    try_cast(value:['_ingested_at'] AS TIMESTAMP)                     AS _ingested_at,
    _metadata.file_size                                               AS _file_size,
    _metadata.file_modification_time                                  AS _file_modified_at
  FROM '${landing_root}/tmdb/trending_movies/'
)
FILEFORMAT = TEXT;
