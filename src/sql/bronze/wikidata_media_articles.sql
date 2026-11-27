-- Bronze table of the raw Wikidata mapping files (landing/wikidata/media_articles/): which Wikipedia article is about
-- each movie and album. Created if missing by: pipeline.py setup
CREATE TABLE IF NOT EXISTS ${catalog}.bronze.wikidata_media_articles (
  media_type         STRING     COMMENT 'movie | music',
  source_id          STRING     COMMENT 'TMDB movie id or MusicBrainz release group id that was looked up',
  wikidata_id        STRING     COMMENT 'Wikidata item, e.g. Q83495',
  article            STRING     COMMENT 'Wikipedia article title with underscores, e.g. The_Matrix',
  url                STRING     COMMENT 'URL of the article',
  dt                 DATE       COMMENT 'ingest date, from the dt= folder',
  batch_id           STRING     COMMENT 'ingest run, from the batch_id= folder',
  _source_file       STRING     COMMENT 'path of the delivered file',
  _ingested_at       TIMESTAMP  COMMENT 'when the API was called (from the file)',
  _file_size         BIGINT     COMMENT 'size in bytes of the delivered file',
  _file_modified_at  TIMESTAMP  COMMENT 'when the delivered file was last written'
)
USING DELTA
LOCATION '${tables_root}/wikidata_media_articles'
COMMENT 'Raw Wikidata mapping of movies and albums to Wikipedia articles: one row per id, article and batch';
