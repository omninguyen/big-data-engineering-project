-- Silver table linking movies to genres, Type 1 (current state only): the genres of each movie's latest TMDB row.
-- Created if missing by: pipeline.py silver. Rebuilt on every run from bronze.tmdb_movies
-- (silver/load/movie_genre.sql).
CREATE TABLE IF NOT EXISTS ${catalog}.silver.movie_genre (
  movie_id    BIGINT     NOT NULL COMMENT 'TMDB id (silver.movie)',
  genre_id    STRING     NOT NULL COMMENT 'silver.genre',
  position    INT        NOT NULL COMMENT 'order in the TMDB list of the movie, 0 = first (main) genre',
  _loaded_at  TIMESTAMP  NOT NULL COMMENT 'when this row was last written to silver',
  CONSTRAINT movie_genre_pk PRIMARY KEY (movie_id, genre_id),
  CONSTRAINT movie_genre_genre_fk FOREIGN KEY (genre_id) REFERENCES ${catalog}.silver.genre
)
USING DELTA
LOCATION '${silver_root}/movie_genre'
COMMENT 'Genres of each TMDB movie, full load';
