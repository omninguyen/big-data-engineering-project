-- Silver table linking albums to genres, Type 1 (current state only), from each album's latest MusicBrainz batch.
-- Created if missing by: pipeline.py setup. Rebuilt on every run (silver/load/release_group_genre.sql).
CREATE TABLE IF NOT EXISTS ${catalog}.silver.release_group_genre (
  release_group_id  STRING     NOT NULL COMMENT 'MusicBrainz release group id (silver.release_group)',
  genre_id          STRING     NOT NULL COMMENT 'silver.genre',
  position          INT        NOT NULL COMMENT '0 = main genre (most tag votes)',
  tag_count         INT        NOT NULL COMMENT 'MusicBrainz tag votes for this genre; 0 = only found by searching it',
  _loaded_at        TIMESTAMP  NOT NULL COMMENT 'when this row was last written to silver',
  CONSTRAINT release_group_genre_pk PRIMARY KEY (release_group_id, genre_id),
  CONSTRAINT release_group_genre_genre_fk FOREIGN KEY (genre_id) REFERENCES ${catalog}.silver.genre
)
USING DELTA
LOCATION '${silver_root}/release_group_genre'
COMMENT 'Genres of each MusicBrainz release group, full load';
