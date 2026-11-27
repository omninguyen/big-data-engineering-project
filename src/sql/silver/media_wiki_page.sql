-- Silver table of the Wikipedia article of each movie and album (Type 1: the latest mapping), one row per media.
-- Created if missing by: pipeline.py setup. Loaded batch by batch from bronze.wikidata_media_articles
-- (silver/load/media_wiki_page.sql).
CREATE TABLE IF NOT EXISTS ${catalog}.silver.media_wiki_page (
  media_type   STRING     NOT NULL COMMENT 'movie | music',
  source_id    STRING     NOT NULL COMMENT 'TMDB movie id (silver.movie) or release group id (silver.release_group)',
  wikidata_id  STRING              COMMENT 'Wikidata item, e.g. Q83495',
  article      STRING     NOT NULL COMMENT 'Wikipedia article title with underscores (silver.wiki_pageview_daily)',
  url          STRING              COMMENT 'URL of the article',
  batch_id     STRING     NOT NULL COMMENT 'bronze batch of this mapping',
  _loaded_at   TIMESTAMP  NOT NULL COMMENT 'when this row was last written to silver',
  CONSTRAINT media_wiki_page_pk PRIMARY KEY (media_type, source_id)
)
USING DELTA
LOCATION '${silver_root}/media_wiki_page'
COMMENT 'Wikipedia article of each movie and album (incremental), from bronze.wikidata_media_articles';
