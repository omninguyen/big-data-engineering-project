"""src/legacy/restructure_bronze.py on a temporary copy of the old bronze layout."""
import contextlib
import io
import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from legacy import restructure_bronze as rb  # noqa: E402

MOVIES = [{"id": 1, "title": "A", "_ingested_at": "2026-11-24T01:30:00.123456+00:00"}, {"id": 2, "title": "B"}]
ALBUMS = [{"id": "x", "_ingested_at": "2026-11-24T02:00:07+00:00"}]


def write(path, text, mtime=None):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    if mtime:
        os.utime(path, (mtime, mtime))


def jsonl(records):
    return "".join(json.dumps(r) + "\n" for r in records)


def files(root):
    return sorted(os.path.relpath(os.path.join(d, f), root) for d, _, fs in os.walk(root) for f in fs)


class RestructureTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        write(f"{self.root}/tmdb/movies/dt=2026-11-24/movies.jsonl", jsonl(MOVIES))
        skipped_at = datetime(2026, 11, 23, 1, 0, 0, tzinfo=timezone.utc).timestamp()
        write(f"{self.root}/tmdb/movies/dt=2026-11-23/skipped.jsonl", "", mtime=skipped_at)
        write(f"{self.root}/musicbrainz/release_groups/dt=2026-11-24/release_groups.jsonl", jsonl(ALBUMS))
        write(f"{self.root}/tmdb/movies/dt=2026-11-26/batch_id=20261126T013005Z-7f3a9c/movies.jsonl", jsonl(MOVIES))
        write(f"{self.root}/manual/tmdb_movies.json", "[]")
        write(f"{self.root}/tmdb/movies/dt=2026-11-24/notes.txt", "?")

    def run_main(self, *args):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = rb.main(["--root", self.root, *args])
        return code, out.getvalue()

    NEW = ["manual/tmdb_movies.json",
           "musicbrainz/release_groups/dt=2026-11-24/batch_id=20261124T020007Z-legacy/release_groups.jsonl",
           "tmdb/movies/dt=2026-11-23/batch_id=20261123T010000Z-legacy/skipped.jsonl",
           "tmdb/movies/dt=2026-11-24/batch_id=20261124T013000Z-legacy/movies.jsonl",
           "tmdb/movies/dt=2026-11-24/notes.txt",
           "tmdb/movies/dt=2026-11-26/batch_id=20261126T013005Z-7f3a9c/movies.jsonl"]

    def test_dry_run_changes_nothing(self):
        before = files(self.root)
        code, out = self.run_main()
        self.assertEqual(code, 0)
        self.assertEqual(files(self.root), before)
        self.assertIn("would move  tmdb/movies/dt=2026-11-24/movies.jsonl -> "
                      "tmdb/movies/dt=2026-11-24/batch_id=20261124T013000Z-legacy/movies.jsonl", out)
        self.assertIn("3 files to move", out)

    def test_apply_moves_old_files_only(self):
        with open(f"{self.root}/tmdb/movies/dt=2026-11-24/movies.jsonl", encoding="utf-8") as f:
            original = f.read()
        code, out = self.run_main("--apply")
        self.assertEqual(code, 0, out)
        self.assertEqual(files(self.root), self.NEW)
        with open(f"{self.root}/{self.NEW[3]}", encoding="utf-8") as f:
            self.assertEqual(f.read(), original)  # contents unchanged
        self.assertIn("left alone  tmdb/movies/dt=2026-11-24/notes.txt", out)
        self.assertNotIn("manual", out)  # never looked at

    def test_running_again_does_nothing(self):
        self.run_main("--apply")
        code, out = self.run_main("--apply")
        self.assertEqual(code, 0)
        self.assertEqual(files(self.root), self.NEW)
        self.assertIn("0 files moved", out)

    def test_resumes_an_interrupted_copy(self):
        source = f"{self.root}/tmdb/movies/dt=2026-11-24/movies.jsonl"
        write(f"{self.root}/{self.NEW[3]}", jsonl(MOVIES))  # copied, original not deleted yet
        code, out = self.run_main("--apply")
        self.assertEqual(code, 0)
        self.assertFalse(os.path.exists(source))
        self.assertIn("resumed", out)

    def test_conflict_keeps_both_files(self):
        source = f"{self.root}/tmdb/movies/dt=2026-11-24/movies.jsonl"
        write(f"{self.root}/{self.NEW[3]}", "different\n")
        code, out = self.run_main()
        self.assertEqual(code, 1)  # the dry run shows it already
        self.assertIn("1 conflicts", out)
        code, out = self.run_main("--apply")
        self.assertEqual(code, 1)
        self.assertTrue(os.path.exists(source))
        self.assertIn("conflict", out)

    def test_missing_root(self):
        self.root = os.path.join(self.root, "nope")
        code, out = self.run_main()
        self.assertEqual(code, 1)
        self.assertIn("not found", out)


class BatchIdTest(unittest.TestCase):
    def test_from_ingested_at_or_file_time(self):
        root = tempfile.mkdtemp()
        write(f"{root}/a.jsonl", jsonl([{"_ingested_at": "2026-11-24T08:30:00+07:00"}]))
        self.assertEqual(rb.legacy_batch_id(f"{root}/a.jsonl"), "20261124T013000Z-legacy")  # converted to UTC
        write(f"{root}/b.jsonl", jsonl([{"id": 1}]), mtime=datetime(2026, 11, 20, tzinfo=timezone.utc).timestamp())
        self.assertEqual(rb.legacy_batch_id(f"{root}/b.jsonl"), "20261120T000000Z-legacy")


if __name__ == "__main__":
    unittest.main()
