-- Incremental load of one bronze batch into silver.wiki_pageview_daily (pipeline.py silver), run once per new batch,
-- oldest first. Every batch holds the last WIKI_DAYS days: new days are added, and a later batch replaces the views
-- of a day it holds again (Wikimedia may still correct a recent day).
MERGE INTO ${catalog}.silver.wiki_pageview_daily AS t
USING (
  SELECT article, view_date, max(views) AS views, max(batch_id) AS batch_id
  FROM ${catalog}.bronze.wikipedia_pageviews
  WHERE batch_id = '${batch_id}' AND article <> '' AND views IS NOT NULL AND view_date IS NOT NULL
  GROUP BY article, view_date
) AS s
ON t.article = s.article AND t.view_date = s.view_date
WHEN MATCHED AND s.batch_id > t.batch_id THEN UPDATE SET
  views = s.views, batch_id = s.batch_id, _loaded_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT
  (article, view_date, views, batch_id, _loaded_at)
  VALUES (s.article, s.view_date, s.views, s.batch_id, current_timestamp());
