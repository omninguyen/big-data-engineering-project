-- Incremental load of one bronze batch into silver.media_wiki_page (pipeline.py silver), run once per new batch,
-- oldest first. A later batch replaces the article of a media. When Wikidata links an id to several articles, the
-- first title is kept.
MERGE INTO ${catalog}.silver.media_wiki_page AS t
USING (
  SELECT
    media_type,
    source_id,
    min_by(wikidata_id, article)  AS wikidata_id,
    min(article)                  AS article,
    min_by(url, article)          AS url,
    max(batch_id)                 AS batch_id
  FROM ${catalog}.bronze.wikidata_media_articles
  WHERE batch_id = '${batch_id}' AND media_type IN ('movie', 'music') AND source_id <> '' AND article <> ''
  GROUP BY media_type, source_id
) AS s
ON t.media_type = s.media_type AND t.source_id = s.source_id
WHEN MATCHED AND s.batch_id > t.batch_id THEN UPDATE SET
  wikidata_id = s.wikidata_id, article = s.article, url = s.url, batch_id = s.batch_id,
  _loaded_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT
  (media_type, source_id, wikidata_id, article, url, batch_id, _loaded_at)
  VALUES (s.media_type, s.source_id, s.wikidata_id, s.article, s.url, s.batch_id, current_timestamp());
