-- Mart view for the dashboard (Preset): the movies and albums with the most Wikipedia views over the last 7 days,
-- next to their TMDB popularity or ListenBrainz listens: three views of public interest.
-- Created or replaced by: pipeline.py marts.
CREATE OR REPLACE VIEW ${catalog}.marts.wiki_attention (
  media_type         COMMENT 'movie | tv | music',
  rank               COMMENT '1 = most viewed article over the last 7 days; filter rank <= 10',
  title              COMMENT 'title',
  creator            COMMENT 'artist credit (music)',
  primary_genre      COMMENT 'primary genre',
  article            COMMENT 'Wikipedia article title',
  url                COMMENT 'URL of the article',
  views_7d           COMMENT 'Wikipedia views over the last 7 days',
  views_prev_7d      COMMENT 'Wikipedia views over the 7 days before',
  views_change_pct   COMMENT 'change of views_7d against views_prev_7d, in percent; NULL when views_prev_7d is 0',
  popularity         COMMENT 'latest TMDB popularity (movies)',
  listen_count       COMMENT 'latest ListenBrainz listens, all-time total (music)',
  window_end         COMMENT 'latest day with Wikipedia views'
)
COMMENT 'Most viewed movies and albums on Wikipedia over 7 days (gold.fact_wiki_pageview_daily, dim_wiki_page)'
AS
WITH daily AS (
  SELECT f.media_key, d.date, f.views, max(d.date) OVER () AS latest_date
  FROM ${catalog}.gold.fact_wiki_pageview_daily AS f
  JOIN ${catalog}.gold.dim_date AS d ON d.date_key = f.date_key
),
weeks AS (
  SELECT
    media_key,
    sum(CASE WHEN date > date_sub(latest_date, 7) THEN views ELSE 0 END)   AS views_7d,
    sum(CASE WHEN date <= date_sub(latest_date, 7) THEN views ELSE 0 END)  AS views_prev_7d,
    max(latest_date)                                                        AS window_end
  FROM daily
  WHERE date > date_sub(latest_date, 14)
  GROUP BY media_key
),
latest_metric AS (
  SELECT
    media_key, popularity, listen_count,
    row_number() OVER (PARTITION BY media_key ORDER BY date_key DESC) AS rn
  FROM ${catalog}.gold.fact_media_metric_daily
)
SELECT
  m.media_type,
  CAST(row_number() OVER (PARTITION BY m.media_type ORDER BY w.views_7d DESC, m.title) AS INT)  AS rank,
  m.title, m.creator, m.primary_genre, p.article, p.url, w.views_7d, w.views_prev_7d,
  round(100 * try_divide(w.views_7d - w.views_prev_7d, w.views_prev_7d), 1)                    AS views_change_pct,
  l.popularity, l.listen_count, w.window_end
FROM weeks AS w
JOIN ${catalog}.gold.dim_media AS m ON m.media_key = w.media_key
JOIN ${catalog}.gold.dim_wiki_page AS p ON p.media_key = w.media_key
LEFT JOIN latest_metric AS l ON l.media_key = w.media_key AND l.rn = 1;
