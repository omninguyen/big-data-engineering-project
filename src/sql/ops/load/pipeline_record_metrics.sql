-- Full load of ops.pipeline_record_metrics (pipeline.py ops): rebuilt on every run, in one write (INSERT OVERWRITE),
-- from every batch in bronze. valid_key is the key silver takes from a row, or NULL when the silver load filters it
-- out: the same predicates as silver/load/movie.sql, release_group.sql, release_group_metric_daily.sql,
-- media_wiki_page.sql and wiki_pageview_daily.sql.
WITH bronze_rows AS (
  SELECT
    'tmdb_movies' AS dataset, 'movie' AS target, batch_id, dt, _source_file, _file_size, _file_modified_at,
    _ingested_at,
    CASE WHEN try_cast(id AS BIGINT) IS NOT NULL THEN CAST(try_cast(id AS BIGINT) AS STRING) END  AS valid_key
  FROM ${catalog}.bronze.tmdb_movies
  UNION ALL
  SELECT
    'musicbrainz_release_groups', 'release_group', batch_id, dt, _source_file, _file_size, _file_modified_at,
    _ingested_at,
    CASE WHEN id <> '' THEN id END
  FROM ${catalog}.bronze.musicbrainz_release_groups
  UNION ALL
  SELECT
    'listenbrainz_release_group_popularity', 'release_group_metric_daily', batch_id, dt, _source_file, _file_size,
    _file_modified_at, _ingested_at,
    CASE WHEN release_group_mbid <> '' AND total_listen_count IS NOT NULL AND dt IS NOT NULL
      THEN release_group_mbid END
  FROM ${catalog}.bronze.listenbrainz_release_group_popularity
  UNION ALL
  SELECT
    'wikidata_media_articles', 'media_wiki_page', batch_id, dt, _source_file, _file_size, _file_modified_at,
    _ingested_at,
    CASE WHEN media_type IN ('movie', 'music') AND source_id <> '' AND article <> ''
      THEN concat(media_type, ':', source_id) END
  FROM ${catalog}.bronze.wikidata_media_articles
  UNION ALL
  SELECT
    'wikipedia_pageviews', 'wiki_pageview_daily', batch_id, dt, _source_file, _file_size, _file_modified_at,
    _ingested_at,
    CASE WHEN article <> '' AND views IS NOT NULL AND view_date IS NOT NULL
      THEN concat(article, '|', CAST(view_date AS STRING)) END
  FROM ${catalog}.bronze.wikipedia_pageviews
),
batches AS (
  SELECT
    dataset, target, batch_id,
    max(dt)                    AS dt,
    count(*)                   AS rows_read,
    count(valid_key)           AS rows_valid,
    count(DISTINCT valid_key)  AS keys_valid,
    max(_file_modified_at)     AS last_file_modified_at,
    max(_ingested_at)          AS last_ingested_at
  FROM bronze_rows
  GROUP BY dataset, target, batch_id
),
files AS (  -- every row carries the size of its file: count each file once
  SELECT dataset, batch_id, count(*) AS files, sum(_file_size) AS bytes
  FROM (SELECT DISTINCT dataset, batch_id, _source_file, _file_size FROM bronze_rows)
  GROUP BY dataset, batch_id
)
INSERT OVERWRITE ${catalog}.ops.pipeline_record_metrics
  (dataset, target, batch_id, dt, rows_read, rows_valid, rows_rejected, rows_duplicate, files, bytes,
   last_file_modified_at, last_ingested_at, silver_loaded_at, silver_lag_s)
SELECT
  b.dataset, b.target, b.batch_id, b.dt, b.rows_read, b.rows_valid,
  b.rows_read - b.rows_valid                                   AS rows_rejected,
  b.rows_valid - b.keys_valid                                  AS rows_duplicate,
  f.files, f.bytes, b.last_file_modified_at, b.last_ingested_at,
  l.loaded_at                                                  AS silver_loaded_at,
  timestampdiff(SECOND, b.last_ingested_at, l.loaded_at)       AS silver_lag_s
FROM batches AS b
JOIN files AS f ON f.dataset = b.dataset AND f.batch_id = b.batch_id
LEFT JOIN ${catalog}.silver.load_log AS l ON l.table_name = b.target AND l.batch_id = b.batch_id;
