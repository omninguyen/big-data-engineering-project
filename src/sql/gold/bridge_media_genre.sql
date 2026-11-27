-- Gold bridge between media and genres: one row per genre of each movie and album (a media has several genres).
-- Created if missing by: pipeline.py setup, after dim_media and dim_genre (its foreign keys).
-- Rebuilt on every run from silver (gold/load/bridge_media_genre.sql).
CREATE TABLE IF NOT EXISTS ${catalog}.gold.bridge_media_genre (
  media_key   STRING   NOT NULL COMMENT 'gold.dim_media',
  genre_id    STRING   NOT NULL COMMENT 'gold.dim_genre',
  is_primary  BOOLEAN  NOT NULL COMMENT 'the primary genre of the media (gold.dim_media.primary_genre), one per media',
  CONSTRAINT bridge_media_genre_pk PRIMARY KEY (media_key, genre_id),
  CONSTRAINT bridge_media_genre_media_fk FOREIGN KEY (media_key) REFERENCES ${catalog}.gold.dim_media,
  CONSTRAINT bridge_media_genre_genre_fk FOREIGN KEY (genre_id) REFERENCES ${catalog}.gold.dim_genre
)
USING DELTA
LOCATION '${gold_root}/bridge_media_genre'
COMMENT 'Genres of each movie, TV show and album, full load';
