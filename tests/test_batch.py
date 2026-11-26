"""ingest.batch: batch ids and the storage key of a batch file."""
import os
import sys
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from ingest.batch import VIETNAM, bronze_key, new_batch_id  # noqa: E402

T0 = datetime(2026, 11, 26, 18, 30, 5, tzinfo=timezone.utc)  # 01:30:05 on 27/11 in Vietnam


class BatchIdTest(unittest.TestCase):
    def test_start_time_then_random_suffix(self):
        ids = {new_batch_id(T0) for _ in range(20)}
        self.assertEqual(len(ids), 20)  # unique within the same second
        for i in ids:
            self.assertRegex(i, r"^20261127T013005-[0-9a-f]{6}$")

    def test_vietnam_time_whatever_the_time_zone(self):
        self.assertEqual(VIETNAM.utcoffset(None), timedelta(hours=7))
        for t in (T0, T0.astimezone(timezone(timedelta(hours=-5)))):
            self.assertTrue(new_batch_id(t).startswith("20261127T013005-"))

    def test_sorts_by_start_time(self):
        self.assertLess(new_batch_id(T0), new_batch_id(T0 + timedelta(seconds=1)))

    def test_now_by_default(self):
        self.assertRegex(new_batch_id(), r"^\d{8}T\d{6}-[0-9a-f]{6}$")


class KeyTest(unittest.TestCase):
    def test_key(self):
        self.assertEqual(bronze_key("tmdb", "movies", "2026-11-27", "20261127T013005-7f3a9c", "movies.jsonl"),
                         "bronze/tmdb/movies/dt=2026-11-27/batch_id=20261127T013005-7f3a9c/movies.jsonl")


if __name__ == "__main__":
    unittest.main()
