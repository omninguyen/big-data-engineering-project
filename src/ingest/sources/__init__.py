"""Ingest sources. Each module has:

  params() -> dict                  settings of the batch, copied into the manifest
  extract(batch) -> Iterator[Page]  records per response; batch.fail_page() for a page that failed,
                                    MissingCredentials to skip, any other exception fails the batch

Sources only fetch: envelopes, files and manifests are written by ingest/bronze.py.
"""
NAMES = ("tmdb", "musicbrainz")


def get(name: str):
    from common import config  # imported here: pipeline.py applies --set settings before config is read
    if name == "musicbrainz":
        from ingest.sources import musicbrainz
        return musicbrainz
    if name == "tmdb":
        if config.MOVIE_SOURCE == "file":  # TMDB export uploaded to MOVIE_FILE
            from ingest.sources import tmdb_file
            return tmdb_file
        if config.MOVIE_SOURCE == "sample":  # built-in demo set, no network needed
            from ingest.sources import sample_movies
            return sample_movies
        from ingest.sources import tmdb
        return tmdb
    raise ValueError(f"unknown source {name!r}, expected one of {NAMES}")
