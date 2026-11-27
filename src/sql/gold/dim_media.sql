-- Gold dimension of media: every TMDB movie and MusicBrainz album, one row each, in its current version
-- (Type 1: the history stays in silver.movie and silver.release_group, SCD2).
-- Created if missing by: pipeline.py setup. Rebuilt on every run from silver (gold/load/dim_media.sql).
CREATE TABLE IF NOT EXISTS ${catalog}.gold.dim_media (
  media_key       STRING  NOT NULL COMMENT '<media_type>:<media_id>, e.g. movie:603, tv:1399; stable across rebuilds',
  media_id        STRING  NOT NULL COMMENT 'natural key: TMDB movie_id or tv_id, or MusicBrainz release_group_id',
  media_type      STRING  NOT NULL COMMENT 'movie | tv | music',
  title           STRING,
  creator         STRING           COMMENT 'artist credit for music, NULL for movies',
  release_date    DATE             COMMENT 'TV shows: first air date; NULL when unknown or not a full date',
  release_year    INT,
  release_decade  INT              COMMENT 'e.g. 1990 for 1990 to 1999',
  primary_genre   STRING           COMMENT 'first TMDB genre of a movie or TV show, most tagged genre of an album',
  genres          STRING           COMMENT 'all genres, comma separated, primary first',
  language        STRING           COMMENT 'ISO 639-1 original language (movies, TV shows)',
  source          STRING  NOT NULL COMMENT 'tmdb | musicbrainz',
  CONSTRAINT dim_media_pk PRIMARY KEY (media_key)
)
USING DELTA
LOCATION '${gold_root}/dim_media'
COMMENT 'Movies, TV shows and albums, current version, full load';
