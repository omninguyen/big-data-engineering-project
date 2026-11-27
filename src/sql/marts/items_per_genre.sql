-- Mart view for the dashboard (Preset): movies, TV shows and albums per genre. Created or replaced by:
-- pipeline.py marts.
CREATE OR REPLACE VIEW ${catalog}.marts.items_per_genre (
  media_type     COMMENT 'movie | tv | music',
  genre_name     COMMENT 'genre as delivered by TMDB or MusicBrainz',
  items          COMMENT 'movies, TV shows or albums with this genre (a media counts once per genre)',
  primary_items  COMMENT 'of them, with this genre as primary genre'
)
COMMENT 'Movies, TV shows and albums per genre (gold.bridge_media_genre, gold.dim_genre)'
AS
SELECT
  g.media_type,
  g.genre_name,
  count(*)                AS items,
  count_if(b.is_primary)  AS primary_items
FROM ${catalog}.gold.bridge_media_genre AS b
JOIN ${catalog}.gold.dim_genre AS g ON g.genre_id = b.genre_id
GROUP BY g.media_type, g.genre_name;
