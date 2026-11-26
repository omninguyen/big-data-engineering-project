"""ingest.batch: batch ids, times, failures and the manifest fields."""
import os
import re
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from common import config  # noqa: E402
from common.errors import SourceError  # noqa: E402
from ingest import batch as batch_mod  # noqa: E402
from ingest.batch import FAILED, MAX_FAILURES_KEPT, RUNNING, SUCCESS, Batch, Page, iso  # noqa: E402

T0 = datetime(2026, 11, 26, 1, 30, 5, 123000, tzinfo=timezone.utc)


class BatchIdTest(unittest.TestCase):
    def test_start_time_then_random_suffix(self):
        ids = {Batch("tmdb", "2026-11-26", started_at=T0).batch_id for _ in range(20)}
        self.assertEqual(len(ids), 20)  # unique within the same second
        for i in ids:
            self.assertRegex(i, r"^20261126T013005Z-[0-9a-f]{6}$")

    def test_sorts_by_start_time(self):
        later = Batch("tmdb", "2026-11-26", started_at=T0 + timedelta(seconds=1)).batch_id
        self.assertLess(Batch("tmdb", "2026-11-26", started_at=T0).batch_id, later)

    def test_times_are_utc_with_z(self):
        self.assertEqual(iso(T0), "2026-11-26T01:30:05.123Z")
        ict = timezone(timedelta(hours=7))
        self.assertEqual(iso(T0.astimezone(ict)), "2026-11-26T01:30:05.123Z")
        self.assertRegex(Page("movies", [], {}).extracted_at, r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$")


class ManifestTest(unittest.TestCase):
    def test_running(self):
        m = Batch("tmdb", "2026-11-26", params={"pages": 2}, started_at=T0).manifest()
        self.assertEqual(m["status"], RUNNING)
        self.assertEqual((m["started_at"], m["finished_at"], m["duration_s"]), ("2026-11-26T01:30:05.123Z", None, None))
        self.assertEqual((m["source"], m["logical_date"], m["params"]), ("tmdb", "2026-11-26", {"pages": 2}))
        self.assertEqual((m["records"], m["pages_failed"], m["failures"], m["error"]), (0, 0, [], None))
        self.assertEqual(m["http"], {"requests": 0, "retries": 0, "status": {}})

    def test_finished(self):
        b = Batch("tmdb", "2026-11-26", started_at=T0)
        b.entities = {"movies": {"records": 800, "files": []}, "genres": {"records": 19, "files": []}}
        with mock.patch.object(batch_mod, "utc_now", return_value=T0 + timedelta(seconds=36.2)):
            b.finish(SUCCESS)
        m = b.manifest()
        self.assertEqual((m["status"], m["finished_at"], m["duration_s"]), (SUCCESS, "2026-11-26T01:30:41.323Z", 36.2))
        self.assertEqual(m["records"], 819)

    def test_error_is_redacted(self):
        b = Batch("tmdb", "2026-11-26")
        b.finish(FAILED, RuntimeError("GET /3/movie/popular?api_key=abc123&page=1 failed"))
        self.assertEqual(b.manifest()["error"],
                         {"type": "RuntimeError", "message": "GET /3/movie/popular?api_key=***&page=1 failed"})
        b.finish(FAILED, SourceError("tmdb", "HTTP 401", status=401))
        self.assertEqual(b.manifest()["error"], {"type": "SourceError", "message": "[tmdb] HTTP 401"})

    def test_failed_pages_are_counted_and_a_few_kept(self):
        b = Batch("musicbrainz", "2026-11-26")
        with self.assertLogs("ingest.batch", "WARNING"):
            for page in range(MAX_FAILURES_KEPT + 5):
                b.fail_page({"query_genre": "jazz", "offset": page * 100}, "Timeout token=s3cret")
        m = b.manifest()
        self.assertEqual(m["pages_failed"], MAX_FAILURES_KEPT + 5)
        self.assertEqual(len(m["failures"]), MAX_FAILURES_KEPT)
        self.assertEqual(m["failures"][0], {"request": {"query_genre": "jazz", "offset": 0}, "reason": "Timeout token=***"})

    def test_databricks_run_ids(self):
        with mock.patch.multiple(config, JOB_ID="11", JOB_RUN_ID="22", TASK_RUN_ID="33", JOB_TRIGGER="periodic"):
            m = Batch("tmdb", "2026-11-26").manifest()
        self.assertEqual(m["databricks"], {"job_id": "11", "job_run_id": "22", "task_run_id": "33"})
        self.assertEqual(m["trigger"], "periodic")
        with mock.patch.multiple(config, JOB_ID="", JOB_RUN_ID="", TASK_RUN_ID="", JOB_TRIGGER=""):
            m = Batch("tmdb", "2026-11-26").manifest()
        self.assertEqual(m["databricks"], {"job_id": None, "job_run_id": None, "task_run_id": None})
        self.assertIsNone(m["trigger"])

    def test_code_version(self):
        version = os.path.join(tempfile.mkdtemp(), "VERSION")
        with mock.patch.object(batch_mod, "_VERSION_FILE", version), mock.patch.object(config, "CODE_VERSION", ""):
            self.assertIsNone(batch_mod.code_version())
            with open(version, "w", encoding="utf-8") as f:
                f.write("a090651\n")
            self.assertEqual(batch_mod.code_version(), "a090651")
            with mock.patch.object(config, "CODE_VERSION", "b22baee"):
                self.assertEqual(batch_mod.code_version(), "b22baee")

    def test_manifest_has_the_documented_fields(self):
        self.assertEqual(set(Batch("tmdb", "2026-11-26").manifest()), {
            "batch_id", "source", "logical_date", "status", "started_at", "finished_at", "duration_s", "trigger",
            "databricks", "code_version", "params", "entities", "records", "http", "pages_failed", "failures",
            "error"})
        self.assertTrue(re.search(r"\bRUNNING\b.*\bSUCCESS\b.*\bPARTIAL\b.*\bFAILED\b.*\bSKIPPED\b",
                                  batch_mod.__doc__, re.S))


if __name__ == "__main__":
    unittest.main()
