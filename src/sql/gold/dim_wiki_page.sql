-- Gold dimension of Wikipedia articles: the article about each movie and album that has one (an outrigger of
-- dim_media, one row per media). Created if missing by: pipeline.py setup, after dim_media (its foreign key).
-- Rebuilt on every run from silver.media_wiki_page (gold/load/dim_wiki_page.sql).
CREATE TABLE IF NOT EXISTS ${catalog}.gold.dim_wiki_page (
  media_key    STRING  NOT NULL COMMENT 'gold.dim_media',
  article      STRING  NOT NULL COMMENT 'Wikipedia article title with underscores, e.g. The_Matrix',
  wikidata_id  STRING           COMMENT 'Wikidata item, e.g. Q83495',
  url          STRING           COMMENT 'URL of the article',
  CONSTRAINT dim_wiki_page_pk PRIMARY KEY (media_key),
  CONSTRAINT dim_wiki_page_media_fk FOREIGN KEY (media_key) REFERENCES ${catalog}.gold.dim_media
)
USING DELTA
LOCATION '${gold_root}/dim_wiki_page'
COMMENT 'Wikipedia article of each movie and album, full load';
