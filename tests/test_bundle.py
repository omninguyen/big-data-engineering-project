"""databricks.yml: what the bundle deploys (only src/) and the daily job, one task per table."""
import os
import re
import sys
import unittest

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
sys.path.insert(0, os.path.join(ROOT, "src"))

import pipeline  # noqa: E402
import sql  # noqa: E402

# The job graph: task -> the tasks it waits for. Each path follows the data, source -> bronze -> silver.
DAG = {
    "setup_tables": set(),
    "ingest_tmdb": set(),
    "ingest_musicbrainz": set(),
    "bronze_tmdb_movies": {"ingest_tmdb", "setup_tables"},
    "bronze_musicbrainz_release_groups": {"ingest_musicbrainz", "setup_tables"},
    "silver_movie": {"bronze_tmdb_movies"},
    "silver_movie_metric_daily": {"bronze_tmdb_movies"},
    "silver_release_group": {"bronze_musicbrainz_release_groups"},
    "silver_artist": {"bronze_musicbrainz_release_groups"},
    "silver_genre": {"bronze_tmdb_movies", "bronze_musicbrainz_release_groups"},
    "silver_movie_genre": {"silver_movie", "silver_genre"},
    "silver_release_group_genre": {"silver_release_group", "silver_genre"},
    "silver_release_group_artist": {"silver_release_group", "silver_artist"},
    "ingest_listenbrainz": {"silver_release_group"},  # looks up the albums known to silver
}

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

    def tasks_and_environments(self):
        tasks = {t["task_key"]: t for t in self.job["tasks"]}
        return tasks, {e["environment_key"] for e in self.job["environments"]}

    def test_the_job_graph(self):
        tasks, environments = self.tasks_and_environments()
        self.assertEqual({key: {d["task_key"] for d in task.get("depends_on", [])} for key, task in tasks.items()},
                         DAG)
        for key, task in tasks.items():
            with self.subTest(task=key):
                self.assertEqual(task["spark_python_task"]["python_file"], "./src/pipeline.py")
                pipeline.parser().parse_args(task["spark_python_task"]["parameters"])  # what pipeline.py accepts
                self.assertIn(task["environment_key"], environments)  # required on serverless
                self.assertGreaterEqual(task["max_retries"], 1)
                self.assertNotIn("run_if", task)  # a task runs only when the tasks it waits for succeeded

    def test_one_task_per_table(self):
        tasks, _ = self.tasks_and_environments()
        loads = {}
        for key, task in tasks.items():
            args = pipeline.parser().parse_args(task["spark_python_task"]["parameters"])
            if args.task in ("bronze", "silver"):
                self.assertEqual(key, f"{args.task}_{args.table}")
                self.assertEqual(dict(args.settings), {"BRONZE_ROOT": "${var.bronze_root}"} if args.task == "bronze"
                                 else {})  # silver reads and writes Unity Catalog tables only
                loads.setdefault(args.task, []).append(args.table)
        self.assertEqual(sorted(loads["bronze"]), sorted(sql.BRONZE_TABLES))
        self.assertEqual(sorted(loads["silver"]), sorted(sql.SILVER_LOADED))

    def test_setup_creates_the_tables_first(self):
        tasks, _ = self.tasks_and_environments()
        args = pipeline.parser().parse_args(tasks["setup_tables"]["spark_python_task"]["parameters"])
        self.assertEqual(args.task, "setup")
        self.assertEqual(dict(args.settings),
                         {"BRONZE_ROOT": "${var.bronze_root}", "SILVER_ROOT": "${var.silver_root}"})

    def test_one_ingest_task_per_source(self):
        tasks, _ = self.tasks_and_environments()
        for key in ("ingest_tmdb", "ingest_musicbrainz", "ingest_listenbrainz"):
            with self.subTest(task=key):
                args = pipeline.parser().parse_args(tasks[key]["spark_python_task"]["parameters"])
                self.assertEqual((args.task, f"ingest_{args.source}"), ("ingest", key))
                self.assertEqual(dict(args.settings), {"BRONZE_ROOT": "${var.bronze_root}"})
        self.assertTrue(os.path.isfile(os.path.join(ROOT, "src", "pipeline.py")))

    def test_job_name(self):
        self.assertEqual(self.job["name"], "media-platform-pipeline-daily")

    def test_daily_at_two_vietnam_time_paused_unless_switched_on(self):
        schedule = self.job["schedule"]
        self.assertEqual(schedule["quartz_cron_expression"], "0 0 2 * * ?")
        self.assertEqual(schedule["timezone_id"], "Asia/Ho_Chi_Minh")
        self.assertEqual(schedule["pause_status"], "${var.schedule_status}")
        self.assertEqual(self.bundle["variables"]["schedule_status"]["default"], "PAUSED")

    def test_no_workspace_or_storage_details_in_the_file(self):
        text = re.sub(r"abfss://\w+@<account>\.dfs\.core\.windows\.net", "", self.text)  # placeholders in descriptions
        for literal in ("abfss://", "azuredatabricks.net", "dfs.core.windows.net"):
            self.assertNotIn(literal, text)
        for variable in ("bronze_root", "silver_root"):  # must come from the secrets
            self.assertNotIn("default", self.bundle["variables"][variable])


if __name__ == "__main__":
    unittest.main()
