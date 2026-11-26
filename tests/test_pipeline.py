"""src/pipeline.py: argument parsing, --set, task dispatch (the batch runner is faked)."""
import os
import sys
import unittest
from unittest import mock

SRC = os.path.join(os.path.dirname(__file__), "..", "src")
sys.path.insert(0, SRC)

import pipeline  # noqa: E402
from ingest import runner  # noqa: E402


class ParserTest(unittest.TestCase):
    def test_dates(self):
        args = pipeline.parser().parse_args(["ingest", "--source", "tmdb", "--date", "2026-11-24"])
        self.assertEqual(args.date, "2026-11-24")
        today = pipeline.parser().parse_args(["ingest", "--source", "tmdb", "--date", ""]).date
        self.assertRegex(today, r"^\d{4}-\d{2}-\d{2}$")  # empty job parameter -> today (UTC)
        with self.assertRaises(SystemExit), mock.patch("sys.stderr"):
            pipeline.parser().parse_args(["ingest", "--source", "tmdb", "--date", "../2026"])

    def test_only_ingest_exists(self):
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


class IngestTest(unittest.TestCase):
    def test_runs_one_batch_of_the_source(self):
        for source in ("tmdb", "musicbrainz"):
            with self.subTest(source=source), \
                    mock.patch.object(runner, "run", return_value={"status": "SUCCESS"}) as run:
                self.assertEqual(pipeline.main(["ingest", "--source", source, "--date", "2026-11-24"]),
                                 {"status": "SUCCESS"})
            run.assert_called_once_with(source, "2026-11-24")

    def test_a_failed_batch_fails_the_task(self):
        with mock.patch.object(runner, "run", side_effect=RuntimeError("boom")), self.assertRaises(RuntimeError):
            pipeline.main(["ingest", "--source", "tmdb"])


if __name__ == "__main__":
    unittest.main()
