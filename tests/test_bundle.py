"""databricks.yml: what the bundle deploys (only src/) and the daily job, one task per table."""
import os
import re
import sys
import unittest

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
sys.path.insert(0, os.path.join(ROOT, "src"))

import pipeline  # noqa: E402
import sql  # noqa: E402

# The job graph: task -> the tasks it waits for. Each path follows the data, source -> bronze -> silver -> gold;
# the ops tasks come last.
DAG = {
    "setup_tables": set(),
    "ingest_tmdb": set(),
    "ingest_musicbrainz": set(),
    "bronze_tmdb_movies": {"ingest_tmdb", "setup_tables"},
    "bronze_tmdb_trending_movies": {"ingest_tmdb", "setup_tables"},  # the same ingest as bronze_tmdb_movies
    "bronze_tmdb_tv_shows": {"ingest_tmdb", "setup_tables"},
    "silver_tv_show": {"bronze_tmdb_tv_shows"},
    "silver_tv_show_metric_daily": {"bronze_tmdb_tv_shows"},
    "silver_tv_show_genre": {"silver_tv_show", "silver_genre"},
    "bronze_musicbrainz_release_groups": {"ingest_musicbrainz", "setup_tables"},
    "silver_movie": {"bronze_tmdb_movies"},
    "silver_movie_metric_daily": {"bronze_tmdb_movies"},
    "silver_release_group": {"bronze_musicbrainz_release_groups"},
    "silver_artist": {"bronze_musicbrainz_release_groups"},
    "silver_genre": {"bronze_tmdb_movies", "bronze_tmdb_tv_shows", "bronze_musicbrainz_release_groups"},
    "silver_movie_genre": {"silver_movie", "silver_genre"},
    "silver_release_group_genre": {"silver_release_group", "silver_genre"},
    "silver_release_group_artist": {"silver_release_group", "silver_artist"},
    "ingest_listenbrainz": {"silver_release_group"},  # looks up the albums known to silver
    "ingest_wikipedia": {"silver_movie", "silver_tv_show", "silver_release_group"},  # looks up the media in silver
    "bronze_wikidata_media_articles": {"ingest_wikipedia", "setup_tables"},
    "bronze_wikipedia_pageviews": {"ingest_wikipedia", "setup_tables"},
    "silver_media_wiki_page": {"bronze_wikidata_media_articles"},
    "silver_wiki_pageview_daily": {"bronze_wikipedia_pageviews"},
    "silver_movie_trending_daily": {"bronze_tmdb_trending_movies"},
    "bronze_listenbrainz_release_group_popularity": {"ingest_listenbrainz", "setup_tables"},
    "silver_release_group_metric_daily": {"bronze_listenbrainz_release_group_popularity"},
    "gold_dim_date": {"setup_tables"},  # the calendar needs no source
    "gold_dim_media": {"silver_movie_genre", "silver_tv_show_genre",  # after movie, tv_show, release_group, genre
                       "silver_release_group_genre"},
    "gold_dim_genre": {"silver_genre"},
    "gold_bridge_media_genre": {"silver_movie_genre", "silver_tv_show_genre", "silver_release_group_genre"},
    "gold_fact_media_metric_daily": {"silver_movie_metric_daily", "silver_tv_show_metric_daily",
                                     "silver_release_group_metric_daily", "gold_dim_media"},  # reads gold.dim_media
    "gold_fact_media_change": {"silver_movie", "silver_tv_show", "silver_release_group"},  # the SCD2 versions
    "gold_dim_wiki_page": {"silver_media_wiki_page", "gold_dim_media"},  # keeps media of dim_media only
    "gold_fact_wiki_pageview_daily": {"silver_wiki_pageview_daily", "gold_dim_wiki_page"},  # media of an article
    "gold_fact_media_trending_daily": {"silver_movie_trending_daily", "gold_dim_media"},  # keeps media of dim_media
    "ops_pipeline_record_metrics": {"silver_movie", "silver_movie_trending_daily", "silver_tv_show",  # load_log
                                    "silver_release_group_metric_daily", "silver_media_wiki_page",
                                    "silver_wiki_pageview_daily"},
    "ops_data_quality_results": {"ops_pipeline_record_metrics"},
    "ops_pipeline_task_runs": {"ops_data_quality_results"},  # near the end: records the runs of every task
    "marts_views": {"setup_tables"},  # views need the tables, not their data
}
# Tasks that also run when a task they wait for failed (the others run only when all of them succeeded).
RUN_IF = {"ops_pipeline_record_metrics": "ALL_DONE",
          "ops_pipeline_task_runs": "ALL_DONE"}  # monitors the batches of a failed path too

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
                self.assertEqual(task.get("run_if"), RUN_IF.get(key))

    def test_one_task_per_table(self):
        tasks, _ = self.tasks_and_environments()
        loads = {}
        for key, task in tasks.items():
            args = pipeline.parser().parse_args(task["spark_python_task"]["parameters"])
            if args.task in ("bronze", "silver", "gold", "ops"):
                self.assertEqual(key, f"{args.task}_{args.table}")
                self.assertEqual(dict(args.settings), {"BRONZE_ROOT": "${var.bronze_root}"} if args.task == "bronze"
                                 else {})  # silver, gold and ops read and write Unity Catalog tables only
                loads.setdefault(args.task, []).append(args.table)
        self.assertEqual(sorted(loads["bronze"]), sorted(sql.BRONZE_TABLES))
        self.assertEqual(sorted(loads["silver"]), sorted(sql.SILVER_LOADED))
        self.assertEqual(sorted(loads["gold"]), sorted(sql.GOLD_TABLES))
        self.assertEqual(sorted(loads["ops"]), sorted([*sql.OPS_TABLES, *sql.OPS_JOB_TABLES]))

    def test_setup_creates_the_tables_first(self):
        tasks, _ = self.tasks_and_environments()
        args = pipeline.parser().parse_args(tasks["setup_tables"]["spark_python_task"]["parameters"])
        self.assertEqual(args.task, "setup")
        self.assertEqual(dict(args.settings), {"BRONZE_ROOT": "${var.bronze_root}", "SILVER_ROOT": "${var.silver_root}",
                                               "GOLD_ROOT": "${var.gold_root}"})

    def test_one_ingest_task_per_source(self):
        tasks, _ = self.tasks_and_environments()
        for key in ("ingest_tmdb", "ingest_musicbrainz", "ingest_listenbrainz", "ingest_wikipedia"):
            with self.subTest(task=key):
                args = pipeline.parser().parse_args(tasks[key]["spark_python_task"]["parameters"])
                self.assertEqual((args.task, f"ingest_{args.source}"), ("ingest", key))
                self.assertEqual(dict(args.settings), {"BRONZE_ROOT": "${var.bronze_root}"})
        self.assertTrue(os.path.isfile(os.path.join(ROOT, "src", "pipeline.py")))

    def test_team_can_view_and_run_the_job_but_not_edit_it(self):
        self.assertEqual(self.job["permissions"], [{"level": "CAN_MANAGE_RUN", "group_name": "media-platform-team"}])
        self.assertNotIn("permissions", self.bundle)  # nothing granted on every resource of the bundle at once

    def test_job_name(self):
        self.assertEqual(self.job["name"], "media-platform-pipeline-daily")

    def test_daily_at_two_vietnam_time_on_unless_switched_off(self):
        schedule = self.job["schedule"]
        self.assertEqual(schedule["quartz_cron_expression"], "0 0 2 * * ?")
        self.assertEqual(schedule["timezone_id"], "Asia/Ho_Chi_Minh")
        self.assertEqual(schedule["pause_status"], "${var.schedule_status}")
        self.assertEqual(self.bundle["variables"]["schedule_status"]["default"], "UNPAUSED")
        with open(os.path.join(ROOT, ".github", "workflows", "cd.yml"), encoding="utf-8") as f:
            self.assertIn("BUNDLE_VAR_schedule_status: ${{ vars.INGEST_SCHEDULE || 'UNPAUSED' }}", f.read())

    def test_marts_task(self):
        tasks, _ = self.tasks_and_environments()
        self.assertEqual(pipeline.parser().parse_args(tasks["marts_views"]["spark_python_task"]["parameters"]).task,
                         "marts")

    def test_no_workspace_or_storage_details_in_the_file(self):
        text = re.sub(r"abfss://\w+@<account>\.dfs\.core\.windows\.net", "", self.text)  # placeholders in descriptions
        for literal in ("abfss://", "azuredatabricks.net", "dfs.core.windows.net"):
            self.assertNotIn(literal, text)
        for variable in ("bronze_root", "silver_root", "gold_root"):  # must come from the secrets
            self.assertNotIn("default", self.bundle["variables"][variable])


if __name__ == "__main__":
    unittest.main()
