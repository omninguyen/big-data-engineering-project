-- Loads new files of landing/wikipedia/pageviews/ into the bronze table (pipeline.py bronze).
-- COPY INTO remembers the files it loaded, so a rerun never loads a file twice. The API gives the day as
-- yyyymmddhh (2026112600): it becomes a DATE, NULL when it is not one.
COPY INTO ${catalog}.bronze.wikipedia_pageviews
FROM (
  SELECT
    value:project                                                     AS project,
    value:article                                                     AS article,
    try_cast(concat_ws('-', substr(value:['timestamp'], 1, 4), substr(value:['timestamp'], 5, 2),
                       substr(value:['timestamp'], 7, 2)) AS DATE)    AS view_date,
    try_cast(value:views AS BIGINT)                                   AS views,
    value:agent                                                       AS agent,
    try_cast(regexp_extract(_metadata.file_path, '/dt=([^/]+)/', 1) AS DATE)  AS dt,
    regexp_extract(_metadata.file_path, '/batch_id=([^/]+)/', 1)      AS batch_id,
    _metadata.file_path                                               AS _source_file,
    try_cast(value:['_ingested_at'] AS TIMESTAMP)                     AS _ingested_at,
    _metadata.file_size                                               AS _file_size,
    _metadata.file_modification_time                                  AS _file_modified_at
  FROM '${landing_root}/wikipedia/pageviews/'
)
FILEFORMAT = TEXT;
