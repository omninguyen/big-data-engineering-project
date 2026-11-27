-- Loads new files of landing/wikidata/media_articles/ into the bronze table (pipeline.py bronze).
-- COPY INTO remembers the files it loaded, so a rerun never loads a file twice.
COPY INTO ${catalog}.bronze.wikidata_media_articles
FROM (
  SELECT
    value:media_type                                                  AS media_type,
    value:source_id                                                   AS source_id,
    value:wikidata_id                                                 AS wikidata_id,
    value:article                                                     AS article,
    value:url                                                         AS url,
    try_cast(regexp_extract(_metadata.file_path, '/dt=([^/]+)/', 1) AS DATE)  AS dt,
    regexp_extract(_metadata.file_path, '/batch_id=([^/]+)/', 1)      AS batch_id,
    _metadata.file_path                                               AS _source_file,
    try_cast(value:['_ingested_at'] AS TIMESTAMP)                     AS _ingested_at,
    _metadata.file_size                                               AS _file_size,
    _metadata.file_modification_time                                  AS _file_modified_at
  FROM '${landing_root}/wikidata/media_articles/'
)
FILEFORMAT = TEXT;
