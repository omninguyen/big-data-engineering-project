"""src/pipeline.py: argument parsing, --set, task dispatch (the task bodies are faked)."""
import os
import sys
import tempfile
import types
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

SRC = os.path.join(os.path.dirname(__file__), "..", "src")
sys.path.insert(0, SRC)

import pipeline  # noqa: E402
from common import config  # noqa: E402
from ingest import batch  # noqa: E402
from common.errors import MissingCredentials  # noqa: E402


def fake_module(**functions):
    return types.SimpleNamespace(**functions)


class ParserTest(unittest.TestCase):
    def test_dates(self):
        args = pipeline.parser().parse_args(["ingest", "--source", "tmdb", "--date", "2026-11-24"])
        self.assertEqual(args.date, "2026-11-24")
        today = pipeline.parser().parse_args(["ingest", "--source", "tmdb", "--date", ""]).date
        self.assertRegex(today, r"^\d{4}-\d{2}-\d{2}$")  # empty job parameter -> today (UTC)
        with self.assertRaises(SystemExit), mock.patch("sys.stderr"):
            pipeline.parser().parse_args(["ingest", "--source", "tmdb", "--date", "../2026"])

    def test_default_date_is_today_in_vietnam(self):
        def at(utc):
            class Clock(datetime):
                @classmethod
                def now(cls, tz=None):
                    return utc.astimezone(tz)
            with mock.patch.object(pipeline, "datetime", Clock):
                return pipeline._date("")
        # 02:00 on 27/11 in Vietnam (the daily schedule) is still 26/11 in UTC
        self.assertEqual(at(datetime(2026, 11, 26, 19, 0, tzinfo=timezone.utc)), "2026-11-27")
        self.assertEqual(at(datetime(2026, 11, 26, 16, 59, tzinfo=timezone.utc)), "2026-11-26")
        self.assertEqual(pipeline.VIETNAM.utcoffset(None), timedelta(hours=7))

    def test_unknown_tasks_are_rejected(self):
        for argv in (["recommend"], ["transform"], ["ingest", "--source", "kafka"]):
            with self.assertRaises(SystemExit), mock.patch("sys.stderr"):
                pipeline.parser().parse_args(argv)
        sub = next(a for a in pipeline.parser()._actions if a.dest == "task")
        self.assertEqual(set(sub.choices), set(pipeline.TASKS))

    def test_settings(self):
        args = pipeline.parser().parse_args(["--set", "UC_CATALOG=c", "--set", "MB_CONTACT=a=b@x",
                                             "ingest", "--source", "tmdb"])
        self.assertEqual(args.settings, [("UC_CATALOG", "c"), ("MB_CONTACT", "a=b@x")])
        for bad in ("UC_CATALOG", "lower=x", "A-B=x"):
            with self.subTest(bad=bad), self.assertRaises(SystemExit), mock.patch("sys.stderr"):
                pipeline.parser().parse_args(["--set", bad, "ingest", "--source", "tmdb"])

    def test_settings_reach_the_environment_before_the_task(self):
        seen = {}
        with mock.patch.dict(pipeline.TASKS, {"ingest": lambda args: seen.update(cat=os.environ["UC_CATALOG"])}), \
                mock.patch.dict(os.environ, {}, clear=False):
            pipeline.main(["--set", "UC_CATALOG=dev_cat", "ingest", "--source", "tmdb"])
        self.assertEqual(seen, {"cat": "dev_cat"})


class TablesTest(unittest.TestCase):
    def test_creates_the_bronze_tables_with_the_spark_session(self):
        session = object()
        pyspark_sql = types.SimpleNamespace(SparkSession=types.SimpleNamespace(
            builder=types.SimpleNamespace(getOrCreate=lambda: session)))
        with mock.patch.dict(sys.modules, {"pyspark": types.ModuleType("pyspark"), "pyspark.sql": pyspark_sql}), \
                mock.patch("sql.create_bronze_tables", return_value=["t"]) as create:
            self.assertEqual(pipeline.main(["tables"]), ["t"])
        create.assert_called_once_with(session)


class IngestTest(unittest.TestCase):
    def ingest(self, source, movie_source="tmdb", tmdb_run=None):
        mods = {"ingest.tmdb": fake_module(run=tmdb_run or (lambda ds, batch_id: 5)),
                "ingest.tmdb_file": fake_module(run=lambda ds, batch_id: 7),
                "ingest.sample_movies": fake_module(run=lambda ds, batch_id: 74),
                "ingest.musicbrainz": fake_module(run=lambda ds, batch_id: 9)}
        import ingest
        with mock.patch.dict(sys.modules, mods), mock.patch.object(config, "MOVIE_SOURCE", movie_source), \
                mock.patch.multiple(ingest, create=True, **{k.split(".")[1]: v for k, v in mods.items()}):
            return pipeline.main(["ingest", "--source", source, "--date", "2026-11-24"])

    def test_sources(self):
        self.assertEqual(self.ingest("musicbrainz"), 9)
        self.assertEqual(self.ingest("tmdb"), 5)
        self.assertEqual(self.ingest("tmdb", movie_source="file"), 7)
        self.assertEqual(self.ingest("tmdb", movie_source="sample"), 74)

    def test_every_run_is_a_new_batch(self):
        seen = []
        for _ in range(2):
            self.ingest("tmdb", tmdb_run=lambda ds, batch_id: seen.append((ds, batch_id)))
        self.assertEqual([ds for ds, _ in seen], ["2026-11-24", "2026-11-24"])
        self.assertNotEqual(seen[0][1], seen[1][1])
        for _, batch_id in seen:
            self.assertRegex(batch_id, r"^\d{8}T\d{6}Z-[0-9a-f]{6}$")

    def test_missing_tmdb_key_succeeds_and_keeps_the_folder_readable(self):
        def no_key(ds, batch_id):
            raise MissingCredentials("TMDB_API_KEY is empty")
        root = tempfile.mkdtemp()
        with mock.patch.object(config, "ON_DATABRICKS", True), mock.patch.object(config, "BRONZE_ROOT", root), \
                mock.patch.object(batch, "new_batch_id", return_value="B1"), self.assertLogs("pipeline", "WARNING"):
            self.assertEqual(self.ingest("tmdb", tmdb_run=no_key), 0)
        path = os.path.join(root, "tmdb", "movies", "dt=2026-11-24", "batch_id=B1", "skipped.jsonl")
        self.assertTrue(os.path.isfile(path))
        self.assertEqual(os.path.getsize(path), 0)


if __name__ == "__main__":
    unittest.main()
