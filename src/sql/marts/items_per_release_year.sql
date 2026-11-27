-- Mart view for the dashboard (Preset): movies, TV shows and albums per release year (TV shows: first air year).
-- Created or replaced by: pipeline.py marts.
CREATE OR REPLACE VIEW ${catalog}.marts.items_per_release_year (
  media_type      COMMENT 'movie | tv | music',
  release_year    COMMENT 'year of release',
  release_decade  COMMENT 'e.g. 1990 for 1990 to 1999',
  items           COMMENT 'movies or albums released, TV shows first aired, that year'
)
COMMENT 'Movies, TV shows and albums per release year, media without a known year left out (gold.dim_media)'
AS
SELECT media_type, release_year, release_decade, count(*) AS items
FROM ${catalog}.gold.dim_media
WHERE release_year IS NOT NULL
GROUP BY media_type, release_year, release_decade;
