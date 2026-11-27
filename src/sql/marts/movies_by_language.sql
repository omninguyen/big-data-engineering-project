-- Mart view for the dashboard (Preset): movies per original language. Created or replaced by: pipeline.py marts.
CREATE OR REPLACE VIEW ${catalog}.marts.movies_by_language (
  language  COMMENT 'ISO 639-1 original language, unknown when missing',
  movies    COMMENT 'movies in this language',
  rank      COMMENT '1 = most movies; filter rank <= 10 for a top 10'
)
COMMENT 'Movies per original language, ranked (gold.dim_media)'
AS
SELECT language, movies, CAST(row_number() OVER (ORDER BY movies DESC, language) AS INT) AS rank
FROM (
  SELECT coalesce(language, 'unknown') AS language, count(*) AS movies
  FROM ${catalog}.gold.dim_media
  WHERE media_type = 'movie'
  GROUP BY coalesce(language, 'unknown')
);
