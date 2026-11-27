-- Full load of gold.dim_wiki_page (pipeline.py gold): rebuilt on every run, in one write (INSERT OVERWRITE), from
-- silver.media_wiki_page. media_key is built as in gold/load/dim_media.sql (movie:<id>, music:<id>), and only media
-- of gold.dim_media are kept, so every row has its dimension.
INSERT OVERWRITE ${catalog}.gold.dim_wiki_page (media_key, article, wikidata_id, url)
SELECT media_key, article, wikidata_id, url
FROM (
  SELECT concat(media_type, ':', source_id) AS media_key, article, wikidata_id, url
  FROM ${catalog}.silver.media_wiki_page
)
WHERE media_key IN (SELECT media_key FROM ${catalog}.gold.dim_media);
