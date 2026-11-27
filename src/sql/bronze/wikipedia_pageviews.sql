-- Bronze table of the raw Wikimedia pageviews files (landing/wikipedia/pageviews/). Every batch holds the last
-- WIKI_DAYS days again, so a day appears in several batches. Created if missing by: pipeline.py setup
CREATE TABLE IF NOT EXISTS ${catalog}.bronze.wikipedia_pageviews (
  project            STRING     COMMENT 'Wikipedia edition, e.g. en.wikipedia',
  article            STRING     COMMENT 'article title with underscores, e.g. The_Matrix',
  view_date          DATE       COMMENT 'day of the views (UTC day, as Wikimedia counts them)',
  views              BIGINT     COMMENT 'views by people that day (agent=user)',
  agent              STRING     COMMENT 'user: bots and crawlers left out',
  dt                 DATE       COMMENT 'ingest date, from the dt= folder',
  batch_id           STRING     COMMENT 'ingest run, from the batch_id= folder',
  _source_file       STRING     COMMENT 'path of the delivered file',
  _ingested_at       TIMESTAMP  COMMENT 'when the API was called (from the file)',
  _file_size         BIGINT     COMMENT 'size in bytes of the delivered file',
  _file_modified_at  TIMESTAMP  COMMENT 'when the delivered file was last written'
)
USING DELTA
LOCATION '${tables_root}/wikipedia_pageviews'
COMMENT 'Raw Wikipedia pageviews: one row per article, day and batch';
