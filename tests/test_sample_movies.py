import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."  , "src"))

from ingest import sample_movies  # noqa: E402


class SampleMoviesTest(unittest.TestCase):
    def setUp(self):
        self.recs = sample_movies.records()

    def test_ids_unique_and_marked_as_sample(self):
        ids = [r["id"] for r in self.recs]
        self.assertEqual(len(ids), len(set(ids)))
        self.assertTrue(all(i.startswith("sample:") for i in ids))

    def test_titles_unique(self):
        titles = [r["title"] for r in self.recs]
        self.assertEqual(len(titles), len(set(titles)))

    def test_fields_match_tmdb_bronze_schema(self):
        need = {"id", "title", "release_date", "original_language", "genres",
                "popularity", "vote_average", "vote_count"}
        for r in self.recs:
            self.assertTrue(need <= set(r))
            self.assertIsInstance(r["genres"], list)
            self.assertTrue(r["genres"])
            self.assertTrue(1900 <= int(r["release_date"][:4]) <= 2026)

    def test_no_invented_numbers(self):
        self.assertTrue(all(r["popularity"] is None and r["vote_average"] is None for r in self.recs))

    def test_genre_names_are_tmdb_style(self):
        known = {"Action", "Adventure", "Animation", "Comedy", "Crime", "Drama", "Family", "Fantasy", "History",
                 "Horror", "Music", "Mystery", "Romance", "Science Fiction", "Thriller", "War"}
        used = {g for r in self.recs for g in r["genres"]}
        self.assertTrue(used <= known, used - known)


if __name__ == "__main__":
    unittest.main()
