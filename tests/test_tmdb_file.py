import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from ingest import tmdb_file  # noqa: E402


def write(doc):
    f = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8")
    json.dump(doc, f)
    f.close()
    return f.name


class TmdbFileTest(unittest.TestCase):
    def test_maps_genre_ids_and_dedupes(self):
        path = write({"genres": {"28": "Action", "12": "Adventure"}, "results": [
            {"id": 1, "title": "A", "genre_ids": [28, 12], "_endpoint": "movie/popular"},
            {"id": 1, "title": "A", "genre_ids": [28], "_endpoint": "movie/top_rated"},
            {"id": 2, "title": "B", "genre_ids": [99]},
            {"id": 3, "title": "", "genre_ids": []},
            {"title": "no id"}]})
        recs = tmdb_file.load(path, now="2026-01-01T00:00:00+00:00")
        self.assertEqual([r["id"] for r in recs], [1, 2])
        self.assertEqual(recs[0]["genres"], ["Action", "Adventure"])
        self.assertEqual(recs[1]["genres"], [])
        self.assertEqual(recs[1]["_endpoint"], "file")
        self.assertEqual(recs[0]["_ingested_at"], "2026-01-01T00:00:00+00:00")

    def test_accepts_plain_list_and_genre_list(self):
        self.assertEqual(len(tmdb_file.load(write([{"id": 5, "title": "X", "genres": ["Drama"]}]))), 1)
        recs = tmdb_file.load(write({"genres": [{"id": 18, "name": "Drama"}],
                                     "results": [{"id": 6, "title": "Y", "genre_ids": [18]}]}))
        self.assertEqual(recs[0]["genres"], ["Drama"])

    def test_missing_file_and_empty_file(self):
        with self.assertRaises(FileNotFoundError):
            tmdb_file.load("/nonexistent/tmdb_movies.json")
        with self.assertRaises(ValueError):
            tmdb_file.load(write({"results": []}))


if __name__ == "__main__":
    unittest.main()
