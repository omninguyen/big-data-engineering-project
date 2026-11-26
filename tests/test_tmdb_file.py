import json
import os
import sys
import tempfile
import unittest
import unittest.mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from common import config  # noqa: E402
from common.errors import SourceError  # noqa: E402
from ingest.batch import Batch  # noqa: E402
from ingest.sources import tmdb_file  # noqa: E402


def write(doc):
    f = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8")
    json.dump(doc, f)
    f.close()
    return f.name


class TmdbFileTest(unittest.TestCase):
    def test_records_as_in_the_file_and_genres_in_the_api_shape(self):
        results = [{"id": 1, "title": "A", "genre_ids": [28, 12]}, {"id": 1, "title": "A", "genre_ids": [28]}]
        movies, genres = tmdb_file.load(write({"genres": {"28": "Action", "12": "Adventure"}, "results": results}))
        self.assertEqual(movies, results)  # duplicates and genre ids are left to transform
        self.assertEqual(genres, [{"id": 28, "name": "Action"}, {"id": 12, "name": "Adventure"}])

    def test_accepts_plain_list_and_genre_list(self):
        self.assertEqual(tmdb_file.load(write([{"id": 5, "title": "X"}])), ([{"id": 5, "title": "X"}], []))
        _, genres = tmdb_file.load(write({"genres": [{"id": 18, "name": "Drama"}], "results": [{"id": 6}]}))
        self.assertEqual(genres, [{"id": 18, "name": "Drama"}])

    def test_missing_file_and_empty_file(self):
        with self.assertRaisesRegex(SourceError, "not found"):
            tmdb_file.load("/nonexistent/tmdb_movies.json")
        with self.assertRaisesRegex(SourceError, "no movies"):
            tmdb_file.load(write({"results": []}))

    def test_pages(self):
        path = write({"genres": [{"id": 18, "name": "Drama"}], "results": [{"id": 6}]})
        with unittest.mock.patch.object(config, "MOVIE_FILE", path):
            pages = list(tmdb_file.extract(Batch("tmdb", "2026-11-26")))
        self.assertEqual([(p.entity, p.request) for p in pages], [("genres", {"file": path}), ("movies", {"file": path})])


if __name__ == "__main__":
    unittest.main()
