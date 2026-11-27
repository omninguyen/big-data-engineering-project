-- Mart view for the dashboard (Preset): what rose most over the last 7 snapshot days, in place of the "trending now"
-- of the streaming design. Albums: listens gained; movies, TV shows: change of TMDB popularity. Only media still in
-- the latest snapshot of their type are ranked. Created or replaced by: pipeline.py marts.
CREATE OR REPLACE VIEW ${catalog}.marts.trending_7d (
  media_type            COMMENT 'movie | tv | music',
  rank                  COMMENT '1 = rose most; filter rank <= 10 for a top 10',
  title                 COMMENT 'title',
  creator               COMMENT 'artist credit (music)',
  primary_genre         COMMENT 'primary genre',
  listens_gained_7d     COMMENT 'ListenBrainz listens gained over the last 7 days (music)',
  popularity_change_7d  COMMENT 'TMDB popularity on the latest day minus the first day of the 7 (movies, TV shows)',
  window_end            COMMENT 'latest snapshot day of this media type'
)
COMMENT 'Movies, TV shows and albums that rose most over the last 7 days (gold.fact_media_metric_daily, gold.dim_media)'
AS
WITH metric AS (
  SELECT
    m.media_type, f.media_key, d.date, f.popularity, f.listen_count_change,
    max(d.date) OVER (PARTITION BY m.media_type) AS latest_date
  FROM ${catalog}.gold.fact_media_metric_daily AS f
  JOIN ${catalog}.gold.dim_media AS m ON m.media_key = f.media_key
  JOIN ${catalog}.gold.dim_date AS d ON d.date_key = f.date_key
),
recent AS (
  SELECT
    media_type, media_key,
    sum(listen_count_change)                                 AS listens_gained_7d,
    max_by(popularity, date) - min_by(popularity, date)      AS popularity_change_7d,
    max(latest_date)                                         AS window_end
  FROM metric
  WHERE date > date_sub(latest_date, 7)
  GROUP BY media_type, media_key
  HAVING max(date) = max(latest_date)
)
SELECT
  r.media_type,
  CAST(row_number() OVER (
    PARTITION BY r.media_type
    ORDER BY CASE WHEN r.media_type = 'music' THEN r.listens_gained_7d ELSE r.popularity_change_7d END DESC NULLS LAST,
             m.title
  ) AS INT) AS rank,
  m.title, m.creator, m.primary_genre, r.listens_gained_7d, r.popularity_change_7d, r.window_end
FROM recent AS r
JOIN ${catalog}.gold.dim_media AS m ON m.media_key = r.media_key;
