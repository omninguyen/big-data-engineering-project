-- Mart view for the dashboard (Preset): the movies of TMDB trending on its latest day, in TMDB's order, with how
-- long they have been trending. Created or replaced by: pipeline.py marts.
CREATE OR REPLACE VIEW ${catalog}.marts.tmdb_trending_today (
  rank                COMMENT '1 = first in TMDB trending (trending/movie/day)',
  rank_previous_day   COMMENT 'rank the day before; NULL when the movie was not trending then',
  title               COMMENT 'title',
  primary_genre       COMMENT 'primary genre',
  release_date        COMMENT 'release date',
  popularity          COMMENT 'TMDB popularity that day',
  days_trending_30d   COMMENT 'days in TMDB trending over the last 30 days, today included',
  first_trending_date COMMENT 'first day the movie was seen in TMDB trending',
  snapshot_date       COMMENT 'latest day of TMDB trending'
)
COMMENT 'Movies of TMDB trending on its latest day (gold.fact_media_trending_daily, gold.dim_media)'
AS
WITH trending AS (
  SELECT f.date_key, f.media_key, f.rank, d.date, max(d.date) OVER () AS latest_date
  FROM ${catalog}.gold.fact_media_trending_daily AS f
  JOIN ${catalog}.gold.dim_date AS d ON d.date_key = f.date_key
),
history AS (
  SELECT
    media_key,
    count_if(date > date_sub(latest_date, 30))  AS days_trending_30d,
    min(date)                                   AS first_trending_date
  FROM trending
  GROUP BY media_key
)
SELECT
  t.rank, p.rank AS rank_previous_day, m.title, m.primary_genre, m.release_date, f.popularity,
  h.days_trending_30d, h.first_trending_date, t.date AS snapshot_date
FROM trending AS t
JOIN ${catalog}.gold.dim_media AS m ON m.media_key = t.media_key
JOIN history AS h ON h.media_key = t.media_key
LEFT JOIN trending AS p ON p.media_key = t.media_key AND p.date = date_sub(t.latest_date, 1)
LEFT JOIN ${catalog}.gold.fact_media_metric_daily AS f ON f.media_key = t.media_key AND f.date_key = t.date_key
WHERE t.date = t.latest_date;
