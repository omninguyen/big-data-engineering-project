"""TMDB movies from a file saved by a browser (MOVIE_SOURCE=file), for networks where Python cannot reach TMDB.

The file (MOVIE_FILE, default <BRONZE_ROOT>/manual/tmdb_movies.json) is a list of TMDB movies, or
{"results": [...], "genres": [{"id": 28, "name": "Action"}, ...] or {"28": "Action", ...}}.
Same entities as the API source (movies, genres); records are written as they are in the file.
"""
import json

from common import config
from common.errors import SourceError
from ingest.batch import Page


def params() -> dict:
    return {"movie_source": "file", "file": config.MOVIE_FILE}


def load(path: str):
    """(movies, genres), genres in the API shape [{"id": 28, "name": "Action"}]."""
    try:
        with open(path, encoding="utf-8") as f:
            doc = json.load(f)
    except FileNotFoundError:
        raise SourceError("tmdb", f"{path} not found - upload the export there, e.g. "
                                  f"databricks fs cp tmdb_movies.json dbfs:{path}") from None
    if isinstance(doc, list):
        movies, genres = doc, []
    else:
        movies, genres = doc.get("results", []), doc.get("genres", [])
    if isinstance(genres, dict):
        genres = [{"id": int(k), "name": v} for k, v in genres.items()]
    if not movies:
        raise SourceError("tmdb", f"no movies found in {path}")
    return movies, genres


def extract(batch):
    movies, genres = load(config.MOVIE_FILE)
    request = {"file": config.MOVIE_FILE}
    if genres:
        yield Page("genres", genres, request)
    yield Page("movies", movies, request)
