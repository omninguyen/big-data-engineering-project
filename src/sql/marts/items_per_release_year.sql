-- Mart view for the dashboard (Preset): movies and albums per release year. Created or replaced by: pipeline.py marts.
CREATE OR REPLACE VIEW ${catalog}.marts.items_per_release_year (
  media_type      COMMENT 'movie | music',
  release_year    COMMENT 'year of release',
  release_decade  COMMENT 'e.g. 1990 for 1990 to 1999',
  items           COMMENT 'movies or albums released that year'
)
COMMENT 'Movies and albums per release year, media without a known year left out (gold.dim_media)'
AS
SELECT media_type, release_year, release_decade, count(*) AS items
FROM ${catalog}.gold.dim_media
WHERE release_year IS NOT NULL
GROUP BY media_type, release_year, release_decade;
