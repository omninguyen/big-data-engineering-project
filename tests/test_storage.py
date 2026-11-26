"""Raw file writes to the landing volume (common.storage): mounted volume on Databricks, Files API elsewhere."""
import json
import os
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from common import config, storage  # noqa: E402

KEY = "bronze/tmdb/movies/dt=2026-10-09/movies.jsonl"


class StorageTest(unittest.TestCase):
    def test_jsonl_bytes(self):
        body, n = storage.jsonl_bytes([{"title": "Phở"}, {"id": 2}])
        self.assertEqual(n, 2)
        self.assertEqual([json.loads(line) for line in body.decode("utf-8").splitlines()], [{"title": "Phở"}, {"id": 2}])
        self.assertEqual(storage.jsonl_bytes([]), (b"", 0))

    def test_volume_path(self):
        with mock.patch.object(config, "UC_LANDING_ROOT", "/Volumes/media/bronze/landing"):
            self.assertEqual(storage.volume_path(KEY), "/Volumes/media/bronze/landing/tmdb/movies/dt=2026-10-09/movies.jsonl")
        with self.assertRaises(ValueError):
            storage.volume_path("silver/movies/x.parquet")

    def test_on_databricks_writes_the_mounted_volume(self):
        root = tempfile.mkdtemp()
        with mock.patch.object(config, "ON_DATABRICKS", True), mock.patch.object(config, "UC_LANDING_ROOT", root):
            self.assertEqual(storage.put_jsonl(KEY, [{"id": 1}, {"id": 2}]), 2)
        with open(os.path.join(root, "tmdb/movies/dt=2026-10-09/movies.jsonl"), encoding="utf-8") as f:
            self.assertEqual(len(f.read().splitlines()), 2)

    def test_outside_databricks_uses_the_files_api(self):
        ws = mock.Mock()
        with mock.patch.object(config, "ON_DATABRICKS", False), \
                mock.patch.object(config, "UC_LANDING_ROOT", "/Volumes/media/bronze/landing"), \
                mock.patch.object(storage, "_workspace", return_value=ws):
            storage.put_jsonl(KEY, [{"id": 1}])
        ws.files.create_directory.assert_called_once_with("/Volumes/media/bronze/landing/tmdb/movies/dt=2026-10-09")
        path, body = ws.files.upload.call_args.args
        self.assertEqual(path, "/Volumes/media/bronze/landing/tmdb/movies/dt=2026-10-09/movies.jsonl")
        self.assertEqual(body.read(), b'{"id": 1}\n')
        self.assertEqual(ws.files.upload.call_args.kwargs, {"overwrite": True})


if __name__ == "__main__":
    unittest.main()
