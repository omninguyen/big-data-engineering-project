"""common.storage: bronze paths; ADLS (abfss://) through dbutils.fs on Databricks, a local folder elsewhere."""
import json
import os
import sys
import tempfile
import types
import unittest
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from common import config, storage  # noqa: E402

ADLS = "abfss://bronze@acct.dfs.core.windows.net"
KEY = "bronze/tmdb/movies/dt=2026-10-09/batch_id=B1/movies.jsonl"


class FakeFs:
    """The part of dbutils.fs used by common.storage, in memory."""

    def __init__(self):
        self.files, self.dirs = {}, []

    def mkdirs(self, path):
        self.dirs.append(path)
        return True

    def put(self, path, contents, overwrite=False):
        if path in self.files and not overwrite:
            raise IOError(f"{path} exists")
        self.files[path] = contents
        return True

    def ls(self, path):
        if path not in self.files:
            raise Exception(f"java.io.FileNotFoundException: {path}")
        return [types.SimpleNamespace(path=path, size=len(self.files[path].encode("utf-8")))]

    def head(self, path, max_bytes=65536):
        return self.files[path].encode("utf-8")[:max_bytes].decode("utf-8")


class PathTest(unittest.TestCase):
    def test_jsonl_bytes(self):
        body, n = storage.jsonl_bytes([{"title": "Phở"}, {"id": 2}])
        self.assertEqual(n, 2)
        self.assertEqual([json.loads(line) for line in body.decode("utf-8").splitlines()], [{"title": "Phở"}, {"id": 2}])
        self.assertEqual(storage.jsonl_bytes([]), (b"", 0))

    def test_bronze_path(self):
        with mock.patch.object(config, "BRONZE_ROOT", ADLS):
            self.assertEqual(storage.bronze_path(KEY), f"{ADLS}/tmdb/movies/dt=2026-10-09/batch_id=B1/movies.jsonl")
            with self.assertRaises(ValueError):
                storage.bronze_path("silver/movies/x.parquet")

    def test_bronze_root_is_required(self):
        with mock.patch.object(config, "BRONZE_ROOT", ""), self.assertRaisesRegex(ValueError, "BRONZE_ROOT"):
            storage.put_jsonl(KEY, [{"id": 1}])


class AdlsTest(unittest.TestCase):
    def setUp(self):
        self.fs = FakeFs()
        for patcher in (mock.patch.multiple(config, ON_DATABRICKS=True, BRONZE_ROOT=ADLS),
                        mock.patch.object(storage, "_dbutils", return_value=types.SimpleNamespace(fs=self.fs))):
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_writes_adls_through_dbutils(self):
        self.assertEqual(storage.put_jsonl(KEY, [{"title": "Phở"}, {"id": 2}]), 2)
        path = f"{ADLS}/tmdb/movies/dt=2026-10-09/batch_id=B1/movies.jsonl"
        self.assertEqual(self.fs.dirs, [f"{ADLS}/tmdb/movies/dt=2026-10-09/batch_id=B1"])
        self.assertEqual(self.fs.files[path], '{"title": "Phở"}\n{"id": 2}\n')

    def test_rewriting_the_same_key_overwrites(self):
        storage.put_bytes(KEY, b"a\n")
        storage.put_bytes(KEY, b"b\n")
        self.assertEqual(list(self.fs.files.values()), ["b\n"])

    def test_read_text(self):
        self.fs.files[f"{ADLS}/manual/tmdb_movies.json"] = '{"results": [{"title": "Phở"}]}'
        self.fs.files[f"{ADLS}/manual/empty.json"] = ""
        self.assertEqual(storage.read_text(f"{ADLS}/manual/tmdb_movies.json"), '{"results": [{"title": "Phở"}]}')
        self.assertEqual(storage.read_text(f"{ADLS}/manual/empty.json"), "")

    def test_abfss_needs_databricks(self):
        with mock.patch.object(config, "ON_DATABRICKS", False), self.assertRaisesRegex(RuntimeError, "local folder"):
            storage.put_bytes(KEY, b"{}\n")
        self.assertEqual(self.fs.files, {})


class LocalFolderTest(unittest.TestCase):
    def test_writes_and_reads_a_local_folder(self):
        root = tempfile.mkdtemp()
        with mock.patch.object(config, "BRONZE_ROOT", root):
            path = storage.put_bytes(KEY, b'{"id": 1}\n')
            self.assertEqual(path, os.path.join(root, "tmdb/movies/dt=2026-10-09/batch_id=B1/movies.jsonl"))
            self.assertEqual(storage.read_text(path), '{"id": 1}\n')
        with self.assertRaises(FileNotFoundError):
            storage.read_text(os.path.join(root, "missing.json"))


if __name__ == "__main__":
    unittest.main()
