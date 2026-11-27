-- Mart view for the dashboard (Preset): movies and albums released, and TV shows first aired, in the last 30 days,
-- and those coming in the next 90 days, ranked by their latest metric (movies, TV shows: TMDB popularity; albums:
-- ListenBrainz listens). Only full release dates count: an album known only by its year or month is left out.
-- Created or replaced by: pipeline.py marts.
CREATE OR REPLACE VIEW ${catalog}.marts.new_releases (
  media_type         COMMENT 'movie | tv | music',
  release_status     COMMENT 'released: in the last 30 days | upcoming: in the next 90 days',
  rank               COMMENT '1 = most popular movie or TV show, most listened album of its status; filter rank <= 10',
  title              COMMENT 'title',
  creator            COMMENT 'artist credit (music)',
  primary_genre      COMMENT 'primary genre',
  release_date       COMMENT 'day of release (TV shows: first air date)',
  days_from_release  COMMENT 'days since the release, negative: days until it',
  first_seen_date    COMMENT 'day the pipeline first saw it (Vietnam time)',
  popularity         COMMENT 'latest TMDB popularity (movies, TV shows)',
  listen_count       COMMENT 'latest ListenBrainz listens, all-time total (music)',
  snapshot_date      COMMENT 'day of the latest metric'
)
COMMENT 'Media released lately or coming soon (gold.dim_media, gold.fact_media_change, metrics)'
AS
WITH today AS (
  SELECT to_date(from_utc_timestamp(current_timestamp(), 'Asia/Ho_Chi_Minh')) AS d
),
first_seen AS (
  SELECT c.media_key, min(d.date) AS first_seen_date
  FROM ${catalog}.gold.fact_media_change AS c
  JOIN ${catalog}.gold.dim_date AS d ON d.date_key = c.date_key
  WHERE c.change_type = 'new'
  GROUP BY c.media_key
),
latest_metric AS (
  SELECT
    f.media_key, f.popularity, f.listen_count, d.date AS snapshot_date,
    row_number() OVER (PARTITION BY f.media_key ORDER BY f.date_key DESC) AS rn
  FROM ${catalog}.gold.fact_media_metric_daily AS f
  JOIN ${catalog}.gold.dim_date AS d ON d.date_key = f.date_key
),
recent AS (
  SELECT
    m.media_key, m.media_type, m.title, m.creator, m.primary_genre, m.release_date,
    datediff(t.d, m.release_date)                                    AS days_from_release,
    CASE WHEN m.release_date > t.d THEN 'upcoming' ELSE 'released' END  AS release_status
  FROM ${catalog}.gold.dim_media AS m
  CROSS JOIN today AS t
  WHERE m.release_date BETWEEN date_sub(t.d, 30) AND date_add(t.d, 90)
)
SELECT
  r.media_type,
  r.release_status,
  CAST(row_number() OVER (
    PARTITION BY r.media_type, r.release_status
    ORDER BY CASE WHEN r.media_type = 'music' THEN l.listen_count ELSE l.popularity END DESC NULLS LAST,
             r.release_date DESC, r.title
  ) AS INT) AS rank,
  r.title, r.creator, r.primary_genre, r.release_date, r.days_from_release, s.first_seen_date,
  l.popularity, l.listen_count, l.snapshot_date
FROM recent AS r
LEFT JOIN first_seen AS s ON s.media_key = r.media_key
LEFT JOIN latest_metric AS l ON l.media_key = r.media_key AND l.rn = 1;
