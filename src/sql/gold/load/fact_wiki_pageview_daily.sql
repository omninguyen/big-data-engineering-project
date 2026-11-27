-- Full load of gold.fact_wiki_pageview_daily (pipeline.py gold): rebuilt on every run, in one write
-- (INSERT OVERWRITE), from silver.wiki_pageview_daily, linked to the media through gold.dim_wiki_page. Two media about
-- the same article (rare) both get its views.
INSERT OVERWRITE ${catalog}.gold.fact_wiki_pageview_daily (date_key, media_key, views)
SELECT
  CAST(date_format(v.view_date, 'yyyyMMdd') AS INT)  AS date_key,
  p.media_key,
  v.views
FROM ${catalog}.silver.wiki_pageview_daily AS v
JOIN ${catalog}.gold.dim_wiki_page AS p ON p.article = v.article;
