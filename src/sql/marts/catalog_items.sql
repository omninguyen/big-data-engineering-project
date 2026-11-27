-- Mart view for the dashboard (Preset): size of the catalog per media type. Created or replaced by: pipeline.py marts.
CREATE OR REPLACE VIEW ${catalog}.marts.catalog_items (
  media_type          COMMENT 'movie | tv | music',
  items               COMMENT 'movies, TV shows or albums in the catalog',
  items_with_genre    COMMENT 'of them, with at least one genre',
  genres              COMMENT 'genres of this media type',
  first_release_year  COMMENT 'oldest release year',
  last_release_year   COMMENT 'newest release year'
)
COMMENT 'Size of the catalog per media type (gold.dim_media, gold.dim_genre)'
AS
SELECT
  m.media_type,
  count(*)                AS items,
  count(m.primary_genre)  AS items_with_genre,
  max(g.genres)           AS genres,
  min(m.release_year)     AS first_release_year,
  max(m.release_year)     AS last_release_year
FROM ${catalog}.gold.dim_media AS m
LEFT JOIN (SELECT media_type, count(*) AS genres FROM ${catalog}.gold.dim_genre GROUP BY media_type) AS g
  ON g.media_type = m.media_type
GROUP BY m.media_type;
