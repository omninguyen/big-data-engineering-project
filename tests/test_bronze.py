"""ingest.bronze: layout, envelope, part files and manifests; local folder, mounted volume, Files API."""
import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from common import config  # noqa: E402
from ingest import bronze  # noqa: E402
from ingest.batch import Batch, Page  # noqa: E402

T0 = datetime(2026, 11, 26, 1, 30, 5, tzinfo=timezone.utc)


def read_jsonl(path):
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f.read().splitlines()]


class LayoutTest(unittest.TestCase):
    def test_paths(self):
        with mock.patch.object(config, "BRONZE_ROOT", "/Volumes/c/bronze/landing"):
            self.assertEqual(bronze.data_dir("tmdb", "movies", "2026-11-26", "20261126T013005Z-7f3a9c"),
                             "/Volumes/c/bronze/landing/tmdb/movies/dt=2026-11-26/batch_id=20261126T013005Z-7f3a9c")
            self.assertEqual(bronze.manifest_path("tmdb", "2026-11-26", "20261126T013005Z-7f3a9c"),
                             "/Volumes/c/bronze/landing/_batches/tmdb/dt=2026-11-26/20261126T013005Z-7f3a9c.json")


class WriterTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        patcher = mock.patch.multiple(config, BRONZE_ROOT=self.root, ON_DATABRICKS=False)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.batch = Batch("tmdb", "2026-11-26", started_at=T0)
        self.dir = os.path.join(self.root, "tmdb", "{}", "dt=2026-11-26", f"batch_id={self.batch.batch_id}")

    def test_envelope_keeps_the_payload_unchanged(self):
        movie = {"id": 1, "title": "Phở", "genre_ids": [28]}
        w = bronze.BatchWriter(self.batch)
        w.write(Page("movies", [movie], {"endpoint": "movie/popular", "page": 1}, extracted_at="2026-11-26T01:30:06.000Z"))
        w.close()
        (line,) = read_jsonl(os.path.join(self.dir.format("movies"), "part-00000.jsonl"))
        self.assertEqual(line, {"_batch_id": self.batch.batch_id, "_source": "tmdb", "_entity": "movies",
                                "_logical_date": "2026-11-26", "_extracted_at": "2026-11-26T01:30:06.000Z",
                                "_request": {"endpoint": "movie/popular", "page": 1}, "payload": movie})

    def test_part_files_per_entity_and_counts(self):
        w = bronze.BatchWriter(self.batch, max_records=2)
        w.write(Page("movies", [{"id": i} for i in range(3)], {"page": 1}))
        w.write(Page("genres", [{"id": 28, "name": "Action"}], {}))
        w.write(Page("movies", [{"id": 3}, {"id": 4}], {"page": 2}))
        w.write(Page("empty", [], {}))
        w.close()
        self.assertEqual(sorted(os.listdir(self.dir.format("movies"))),
                         ["part-00000.jsonl", "part-00001.jsonl", "part-00002.jsonl"])
        ids = [r["payload"]["id"] for name in sorted(os.listdir(self.dir.format("movies")))
               for r in read_jsonl(os.path.join(self.dir.format("movies"), name))]
        self.assertEqual(ids, [0, 1, 2, 3, 4])
        e = self.batch.entities
        self.assertEqual((e["movies"]["records"], e["genres"]["records"], e["empty"]["records"]), (5, 1, 0))
        self.assertEqual([(f["name"], f["records"]) for f in e["movies"]["files"]],
                         [("part-00000.jsonl", 2), ("part-00001.jsonl", 2), ("part-00002.jsonl", 1)])
        self.assertTrue(all(f["bytes"] > 0 for f in e["movies"]["files"]))
        self.assertFalse(os.path.exists(self.dir.format("empty")))
        self.assertEqual(self.batch.records, 6)

    def test_manifest_is_one_json_line_and_is_rewritten(self):
        path = bronze.write_manifest(self.batch)
        self.assertEqual(path, os.path.join(self.root, "_batches", "tmdb", "dt=2026-11-26", f"{self.batch.batch_id}.json"))
        self.assertEqual(read_jsonl(path)[0]["status"], "RUNNING")
        self.batch.finish("SUCCESS")
        bronze.write_manifest(self.batch)
        (m,) = read_jsonl(path)
        self.assertEqual(m["status"], "SUCCESS")


class BackendTest(unittest.TestCase):
    def test_on_databricks_writes_the_mounted_volume(self):
        root = tempfile.mkdtemp()
        with mock.patch.multiple(config, ON_DATABRICKS=True, BRONZE_ROOT=root):
            bronze.put_bytes(f"{root}/tmdb/x.jsonl", b"{}\n")
        with open(os.path.join(root, "tmdb", "x.jsonl"), "rb") as f:
            self.assertEqual(f.read(), b"{}\n")

    def test_volume_outside_databricks_uses_the_files_api(self):
        ws = mock.Mock()
        with mock.patch.object(config, "ON_DATABRICKS", False), mock.patch.object(bronze, "_workspace", return_value=ws):
            bronze.put_bytes("/Volumes/c/bronze/landing/tmdb/movies/part-00000.jsonl", b'{"id": 1}\n')
        ws.files.create_directory.assert_called_once_with("/Volumes/c/bronze/landing/tmdb/movies")
        path, body = ws.files.upload.call_args.args
        self.assertEqual(path, "/Volumes/c/bronze/landing/tmdb/movies/part-00000.jsonl")
        self.assertEqual(body.read(), b'{"id": 1}\n')
        self.assertEqual(ws.files.upload.call_args.kwargs, {"overwrite": True})


if __name__ == "__main__":
    unittest.main()
