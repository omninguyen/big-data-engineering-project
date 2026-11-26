"""databricks.yml: what the bundle deploys (only src/) and the daily ingest job."""
import os
import sys
import unittest

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
sys.path.insert(0, os.path.join(ROOT, "src"))

import pipeline  # noqa: E402

try:
    import yaml
except ImportError:  # pyyaml is installed in CI
    yaml = None


@unittest.skipIf(yaml is None, "pyyaml not installed")
class BundleTest(unittest.TestCase):
    def setUp(self):
        with open(os.path.join(ROOT, "databricks.yml"), encoding="utf-8") as f:
            self.text = f.read()
        self.bundle = yaml.safe_load(self.text)
        self.job = self.bundle["resources"]["jobs"]["ingest"]

    def test_only_src_is_deployed_under_shared(self):
        self.assertEqual(self.bundle["sync"]["paths"], ["src"])
        workspace = self.bundle["targets"]["prod"]["workspace"]
        self.assertTrue(workspace["root_path"].startswith("/Shared/"))
        self.assertTrue(workspace["file_path"].startswith("/Shared/"))
        self.assertNotIn("host", workspace)  # from the DATABRICKS_HOST secret

    def test_one_task_per_source_running_pipeline_py(self):
        tasks = {t["task_key"]: t for t in self.job["tasks"]}
        self.assertEqual(set(tasks), {"ingest_tmdb", "ingest_musicbrainz"})
        environments = {e["environment_key"] for e in self.job["environments"]}
        for key, task in tasks.items():
            with self.subTest(task=key):
                python = task["spark_python_task"]
                self.assertEqual(python["python_file"], "./src/pipeline.py")
                args = pipeline.parser().parse_args(python["parameters"])  # what pipeline.py accepts
                self.assertEqual((args.task, f"ingest_{args.source}"), ("ingest", key))
                self.assertEqual(dict(args.settings), {"BRONZE_ROOT": "${var.bronze_root}"})
                self.assertNotIn("depends_on", task)  # the sources run in parallel
                self.assertIn(task["environment_key"], environments)  # required on serverless
                self.assertGreaterEqual(task["max_retries"], 1)
        self.assertTrue(os.path.isfile(os.path.join(ROOT, "src", "pipeline.py")))

    def test_daily_at_two_vietnam_time_paused_unless_switched_on(self):
        schedule = self.job["schedule"]
        self.assertEqual(schedule["quartz_cron_expression"], "0 0 2 * * ?")
        self.assertEqual(schedule["timezone_id"], "Asia/Ho_Chi_Minh")
        self.assertEqual(schedule["pause_status"], "${var.schedule_status}")
        self.assertEqual(self.bundle["variables"]["schedule_status"]["default"], "PAUSED")

    def test_no_workspace_or_storage_details_in_the_file(self):
        for literal in ("abfss://bronze@", "azuredatabricks.net", "dfs.core.windows.net"):
            self.assertNotIn(literal, self.text.replace("abfss://bronze@<account>.dfs.core.windows.net", ""))
        self.assertNotIn("default", self.bundle["variables"]["bronze_root"])  # must come from the secret


if __name__ == "__main__":
    unittest.main()
