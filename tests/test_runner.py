"""ingest.runner: one batch end to end with a fake source, written to a temporary bronze folder."""
import json
import logging
import os
import sys
import tempfile
import types
import unittest
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from common import config, logs  # noqa: E402
from common.errors import MissingCredentials, SourceError  # noqa: E402
from ingest import runner, sources  # noqa: E402
from ingest.batch import Page  # noqa: E402

DATE = "2026-11-26"


def fake_source(extract):
    return types.SimpleNamespace(params=lambda: {"pages": 2}, extract=extract)


class RunnerTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        patcher = mock.patch.multiple(config, BRONZE_ROOT=self.root, ON_DATABRICKS=False)
        patcher.start()
        self.addCleanup(patcher.stop)

    def run_source(self, extract):
        with mock.patch.object(sources, "get", return_value=fake_source(extract)):
            return runner.run("tmdb", DATE)

    def manifests(self):
        folder = os.path.join(self.root, "_batches", "tmdb", f"dt={DATE}")
        out = {}
        for name in sorted(os.listdir(folder)):
            with open(os.path.join(folder, name), encoding="utf-8") as f:
                out[name[:-len(".json")]] = json.loads(f.read())
        return out

    def test_success(self):
        def extract(batch):
            yield Page("genres", [{"id": 28, "name": "Action"}], {"endpoint": "genre/movie/list"})
            yield Page("movies", [{"id": 1}, {"id": 2}], {"endpoint": "movie/popular", "page": 1})
        with self.assertLogs("ingest.runner", "INFO") as cm:
            m = self.run_source(extract)
        self.assertEqual((m["status"], m["records"], m["params"]), ("SUCCESS", 3, {"pages": 2}))
        self.assertEqual({k: v["records"] for k, v in m["entities"].items()}, {"genres": 1, "movies": 2})
        self.assertEqual(self.manifests(), {m["batch_id"]: m})
        part = os.path.join(self.root, "tmdb", "movies", f"dt={DATE}", f"batch_id={m['batch_id']}", "part-00000.jsonl")
        self.assertTrue(os.path.isfile(part))
        self.assertIn("tmdb batch SUCCESS: 3 records (genres 1, movies 2)", cm.output[-1])

    def test_manifest_says_running_while_the_source_runs(self):
        seen = {}

        def extract(batch):
            seen.update(self.manifests())
            yield Page("movies", [{"id": 1}], {})
        m = self.run_source(extract)
        self.assertEqual(seen[m["batch_id"]]["status"], "RUNNING")

    def test_partial(self):
        def extract(batch):
            yield Page("movies", [{"id": 1}], {"page": 1})
            batch.fail_page({"page": 2}, "HTTP 500")
        with self.assertLogs("ingest", "WARNING"):
            m = self.run_source(extract)
        self.assertEqual((m["status"], m["pages_failed"], m["records"]), ("PARTIAL", 1, 1))

    def test_skipped(self):
        def extract(batch):
            raise MissingCredentials("TMDB_API_KEY is empty")
            yield  # a generator, like the real sources
        with self.assertLogs("ingest.runner", "WARNING"):
            m = self.run_source(extract)
        self.assertEqual((m["status"], m["records"]), ("SKIPPED", 0))
        self.assertEqual(m["error"], {"type": "MissingCredentials", "message": "TMDB_API_KEY is empty"})
        self.assertFalse(os.path.exists(os.path.join(self.root, "tmdb")))
        self.assertEqual(self.manifests()[m["batch_id"]]["status"], "SKIPPED")

    def test_failed_raises_after_writing_the_manifest(self):
        def extract(batch):
            yield Page("movies", [{"id": 1}], {"page": 1})
            raise SourceError("tmdb", "GET movie/popular?api_key=abc123 -> HTTP 401", status=401)
        with self.assertRaises(SourceError), self.assertLogs("ingest.runner", "ERROR"):
            self.run_source(extract)
        (m,) = self.manifests().values()
        self.assertEqual(m["status"], "FAILED")
        self.assertEqual(m["error"]["type"], "SourceError")
        self.assertNotIn("abc123", json.dumps(m))
        self.assertIsNotNone(m["finished_at"])

    def test_rerun_is_a_new_batch(self):
        def extract(batch):
            yield Page("movies", [{"id": 1}], {})
        first, second = self.run_source(extract), self.run_source(extract)
        self.assertNotEqual(first["batch_id"], second["batch_id"])
        self.assertEqual(set(self.manifests()), {first["batch_id"], second["batch_id"]})
        movies = os.path.join(self.root, "tmdb", "movies", f"dt={DATE}")
        self.assertEqual(len(os.listdir(movies)), 2)

    def test_log_records_carry_the_batch_id(self):
        logs.install()

        def extract(batch):
            logging.getLogger("ingest.sources.fake").info("page 1")
            yield Page("movies", [{"id": 1}], {})
        with self.assertLogs(level="INFO") as cm:
            m = self.run_source(extract)
        self.assertEqual({r.batch_id for r in cm.records}, {m["batch_id"]})
        self.assertEqual(logging.makeLogRecord({}).__dict__.get("batch_id", "-"), "-")


if __name__ == "__main__":
    unittest.main()
