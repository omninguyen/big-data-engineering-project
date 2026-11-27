-- Gold dimension of genres: the TMDB genres of movies and the MusicBrainz genres of albums, one row each.
-- Created if missing by: pipeline.py setup. Rebuilt on every run from silver.genre (gold/load/dim_genre.sql).
CREATE TABLE IF NOT EXISTS ${catalog}.gold.dim_genre (
  genre_id    STRING  NOT NULL COMMENT '<source>:<lower-case name>, e.g. tmdb:science fiction, musicbrainz:rock',
  genre_name  STRING  NOT NULL COMMENT 'display name as delivered',
  media_type  STRING  NOT NULL COMMENT 'movie (TMDB genres) | tv (TMDB TV genres) | music (MusicBrainz genres)',
  CONSTRAINT dim_genre_pk PRIMARY KEY (genre_id)
)
USING DELTA
LOCATION '${gold_root}/dim_genre'
COMMENT 'Genres of movies and albums, full load';
