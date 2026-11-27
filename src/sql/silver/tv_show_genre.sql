-- Silver table linking TV shows to genres, Type 1 (current state only): the genres of each show's latest TMDB row.
-- Created if missing by: pipeline.py setup. Rebuilt on every run from bronze.tmdb_tv_shows
-- (silver/load/tv_show_genre.sql).
CREATE TABLE IF NOT EXISTS ${catalog}.silver.tv_show_genre (
  tv_id       BIGINT     NOT NULL COMMENT 'TMDB TV id (silver.tv_show)',
  genre_id    STRING     NOT NULL COMMENT 'silver.genre, tmdb_tv:<name>',
  position    INT        NOT NULL COMMENT 'order in the TMDB list of the show, 0 = first (main) genre',
  _loaded_at  TIMESTAMP  NOT NULL COMMENT 'when this row was last written to silver',
  CONSTRAINT tv_show_genre_pk PRIMARY KEY (tv_id, genre_id),
  CONSTRAINT tv_show_genre_genre_fk FOREIGN KEY (genre_id) REFERENCES ${catalog}.silver.genre
)
USING DELTA
LOCATION '${silver_root}/tv_show_genre'
COMMENT 'Genres of each TMDB TV show, full load';
