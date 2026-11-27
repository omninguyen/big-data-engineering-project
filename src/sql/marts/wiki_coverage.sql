-- Mart view for the dashboard (Preset): how many movies, TV shows and albums have a Wikipedia article. Created or
-- replaced by: pipeline.py marts.
CREATE OR REPLACE VIEW ${catalog}.marts.wiki_coverage (
  media_type          COMMENT 'movie | tv | music',
  items               COMMENT 'movies or albums in the catalog',
  items_with_article  COMMENT 'of them, with a Wikipedia article',
  coverage_pct        COMMENT 'items_with_article / items, in percent'
)
COMMENT 'Share of movies and albums with a Wikipedia article (gold.dim_media, gold.dim_wiki_page)'
AS
SELECT
  m.media_type,
  count(*)                                                     AS items,
  count(p.media_key)                                           AS items_with_article,
  round(100 * try_divide(count(p.media_key), count(*)), 1)     AS coverage_pct
FROM ${catalog}.gold.dim_media AS m
LEFT JOIN ${catalog}.gold.dim_wiki_page AS p ON p.media_key = m.media_key
GROUP BY m.media_type;
