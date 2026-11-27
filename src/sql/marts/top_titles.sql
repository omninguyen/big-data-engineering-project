-- Mart view for the dashboard (Preset): movies, TV shows and albums of the latest snapshot day, ranked by their main
-- metric (movies, TV shows: TMDB popularity; albums: ListenBrainz listens). Created or replaced by: pipeline.py marts.
CREATE OR REPLACE VIEW ${catalog}.marts.top_titles (
  media_type      COMMENT 'movie | tv | music',
  rank            COMMENT '1 = most popular movie or TV show, most listened album; filter rank <= 10 for a top 10',
  title           COMMENT 'title',
  creator         COMMENT 'artist credit (music)',
  primary_genre   COMMENT 'primary genre',
  release_year    COMMENT 'year of release',
  popularity      COMMENT 'TMDB popularity (movies, TV shows)',
  rating          COMMENT 'TMDB vote average 0-10 (movies, TV shows)',
  votes           COMMENT 'TMDB vote count (movies, TV shows)',
  listen_count    COMMENT 'ListenBrainz listens, all-time total (music)',
  listener_count  COMMENT 'ListenBrainz listeners, all-time total (music)',
  snapshot_date   COMMENT 'latest snapshot day of this media type'
)
COMMENT 'Movies, TV shows and albums of the latest snapshot day, ranked (gold.fact_media_metric_daily, gold.dim_media)'
AS
WITH latest AS (
  SELECT
    m.media_type, m.title, m.creator, m.primary_genre, m.release_year,
    f.popularity, f.rating, f.votes, f.listen_count, f.listener_count,
    d.date                                         AS snapshot_date,
    max(d.date) OVER (PARTITION BY m.media_type)  AS latest_date
  FROM ${catalog}.gold.fact_media_metric_daily AS f
  JOIN ${catalog}.gold.dim_media AS m ON m.media_key = f.media_key
  JOIN ${catalog}.gold.dim_date AS d ON d.date_key = f.date_key
)
SELECT
  media_type,
  CAST(row_number() OVER (
    PARTITION BY media_type
    ORDER BY CASE WHEN media_type = 'music' THEN listen_count ELSE popularity END DESC NULLS LAST, title
  ) AS INT) AS rank,
  title, creator, primary_genre, release_year, popularity, rating, votes, listen_count, listener_count, snapshot_date
FROM latest
WHERE snapshot_date = latest_date;
