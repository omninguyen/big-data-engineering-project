"""src/pipeline.py: argument parsing, --set, task dispatch (the task bodies are faked)."""
import os
import sys
import tempfile
import types
import unittest
from datetime import datetime, timezone
from unittest import mock

SRC = os.path.join(os.path.dirname(__file__), "..", "src")
sys.path.insert(0, SRC)

import pipeline  # noqa: E402
from common import config  # noqa: E402
from ingest import batch  # noqa: E402
from common.errors import MissingCredentials  # noqa: E402


def fake_module(**functions):
    return types.SimpleNamespace(**functions)


def clock(utc: datetime):
    """Freeze the time of ingest.batch (default dt and batch id) at `utc`."""
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return utc.astimezone(tz)
    return mock.patch.object(batch, "datetime", Clock)


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
            with clock(utc):
                return pipeline._date("")
        # 02:00 on 27/11 in Vietnam (the daily schedule) is still 26/11 in UTC
        self.assertEqual(at(datetime(2026, 11, 26, 19, 0, tzinfo=timezone.utc)), "2026-11-27")
        self.assertEqual(at(datetime(2026, 11, 26, 16, 59, tzinfo=timezone.utc)), "2026-11-26")

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
    def run_with(self, argv, function):
        """Run pipeline.py with `argv`, faking Spark and sql.<function>; returns (result, the call's arguments)."""
        session = object()
        pyspark_sql = types.SimpleNamespace(SparkSession=types.SimpleNamespace(
            builder=types.SimpleNamespace(getOrCreate=lambda: session)))
        with mock.patch.dict(sys.modules, {"pyspark": types.ModuleType("pyspark"), "pyspark.sql": pyspark_sql}), \
                mock.patch(f"sql.{function}", return_value={"t": 1}) as fake:
            result = pipeline.main(argv)
        fake.assert_called_once()
        self.assertIs(fake.call_args.args[0], session)
        return result, fake.call_args.args[1:]

    def test_setup_creates_every_table(self):
        self.assertEqual(self.run_with(["setup"], "create_tables"), ({"t": 1}, ()))

    def test_one_table_or_the_whole_layer(self):
        self.assertEqual(self.run_with(["bronze", "--table", "tmdb_movies"], "load_bronze_table"),
                         ({"t": 1}, ("tmdb_movies",)))
        self.assertEqual(self.run_with(["bronze"], "load_bronze_tables"), ({"t": 1}, ()))
        self.assertEqual(self.run_with(["silver", "--table", "movie_genre"], "load_silver_table"),
                         ({"t": 1}, ("movie_genre",)))
        self.assertEqual(self.run_with(["silver"], "load_silver_tables"), ({"t": 1}, ()))
        self.assertEqual(self.run_with(["gold", "--table", "dim_media"], "load_gold_table"),
                         ({"t": 1}, ("dim_media",)))
        self.assertEqual(self.run_with(["gold"], "load_gold_tables"), ({"t": 1}, ()))
        self.assertEqual(self.run_with(["ops", "--table", "data_quality_results"], "load_ops_table"),
                         ({"t": 1}, ("data_quality_results",)))
        self.assertEqual(self.run_with(["ops"], "load_ops_tables"), ({"t": 1}, ()))

    def test_marts_creates_the_views(self):
        self.assertEqual(self.run_with(["marts"], "create_marts_views"), ({"t": 1}, ()))

    def test_unknown_table_is_rejected(self):
        for argv in (["bronze", "--table", "movie"], ["silver", "--table", "load_log"], ["silver", "--table", "x"],
                     ["gold", "--table", "movie"], ["ops", "--table", "dim_date"]):
            with self.subTest(argv=argv), self.assertRaises(SystemExit), mock.patch("sys.stderr"):
                pipeline.parser().parse_args(argv)


class IngestTest(unittest.TestCase):
    def ingest(self, source, movie_source="tmdb", tmdb_run=None):
        mods = {"ingest.tmdb": fake_module(run=tmdb_run or (lambda ds, batch_id: 5)),
                "ingest.tmdb_file": fake_module(run=lambda ds, batch_id: 7),
                "ingest.sample_movies": fake_module(run=lambda ds, batch_id: 74),
                "ingest.musicbrainz": fake_module(run=lambda ds, batch_id: 9),
                "ingest.wikipedia": fake_module(run=lambda ds, batch_id: 11)}
        import ingest
        with mock.patch.dict(sys.modules, mods), mock.patch.object(config, "MOVIE_SOURCE", movie_source), \
                mock.patch.multiple(ingest, create=True, **{k.split(".")[1]: v for k, v in mods.items()}):
            return pipeline.main(["ingest", "--source", source, "--date", "2026-11-24"])

    def test_sources(self):
        self.assertEqual(self.ingest("musicbrainz"), 9)
        self.assertEqual(self.ingest("wikipedia"), 11)
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
            self.assertRegex(batch_id, r"^\d{8}T\d{6}-[0-9a-f]{6}$")

    def test_default_dt_and_batch_id_show_the_same_day(self):
        seen = []
        mods = {"ingest.tmdb": fake_module(run=lambda ds, batch_id: seen.append((ds, batch_id)))}
        import ingest
        with clock(datetime(2026, 11, 26, 19, 0, 5, tzinfo=timezone.utc)), mock.patch.dict(sys.modules, mods), \
                mock.patch.object(config, "MOVIE_SOURCE", "tmdb"), \
                mock.patch.object(ingest, "tmdb", mods["ingest.tmdb"], create=True):
            pipeline.main(["ingest", "--source", "tmdb"])
        ds, batch_id = seen[0]
        self.assertEqual(ds, "2026-11-27")
        self.assertRegex(batch_id, r"^20261127T020005-[0-9a-f]{6}$")

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
