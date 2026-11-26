"""ingest.batch: batch ids and the storage key of a batch file."""
import os
import sys
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from ingest.batch import bronze_key, new_batch_id  # noqa: E402

T0 = datetime(2026, 11, 26, 1, 30, 5, tzinfo=timezone.utc)


class BatchIdTest(unittest.TestCase):
    def test_start_time_then_random_suffix(self):
        ids = {new_batch_id(T0) for _ in range(20)}
        self.assertEqual(len(ids), 20)  # unique within the same second
        for i in ids:
            self.assertRegex(i, r"^20261126T013005Z-[0-9a-f]{6}$")

    def test_utc_whatever_the_time_zone(self):
        ict = timezone(timedelta(hours=7))
        self.assertTrue(new_batch_id(T0.astimezone(ict)).startswith("20261126T013005Z-"))

    def test_sorts_by_start_time(self):
        self.assertLess(new_batch_id(T0), new_batch_id(T0 + timedelta(seconds=1)))

    def test_now_by_default(self):
        self.assertRegex(new_batch_id(), r"^\d{8}T\d{6}Z-[0-9a-f]{6}$")


class KeyTest(unittest.TestCase):
    def test_key(self):
        self.assertEqual(bronze_key("tmdb", "movies", "2026-11-26", "20261126T013005Z-7f3a9c", "movies.jsonl"),
                         "bronze/tmdb/movies/dt=2026-11-26/batch_id=20261126T013005Z-7f3a9c/movies.jsonl")


if __name__ == "__main__":
    unittest.main()
