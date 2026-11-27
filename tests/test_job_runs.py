"""Task runs from the Jobs API (src/job_runs.py) with a fake workspace client."""
import os
import sys
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace as NS
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
import job_runs  # noqa: E402

NOW = datetime(2026, 11, 27, 3, 0, tzinfo=timezone.utc)
T0 = int(datetime(2026, 11, 26, 19, 0, tzinfo=timezone.utc).timestamp() * 1000)


class FakeJobs:
    def __init__(self, runs):
        self.runs, self.calls = runs, []

    def list_runs(self, **kwargs):
        self.calls.append(kwargs)
        return iter(self.runs)


class TaskRunsTest(unittest.TestCase):
    def test_one_row_per_task_attempt(self):
        done = NS(run_id=11, task_key="silver_movie", attempt_number=1, start_time=T0, end_time=T0 + 90_000,
                  state=NS(result_state=NS(value="FAILED"), life_cycle_state=NS(value="TERMINATED"),
                           state_message="boom"))
        running = NS(run_id=12, task_key="gold_dim_media", attempt_number=0, start_time=T0, end_time=0,
                     state=NS(result_state=None, life_cycle_state=NS(value="RUNNING"), state_message=""))
        jobs = FakeJobs([NS(run_id=1, trigger=NS(value="PERIODIC"), tasks=[done, running]), NS(run_id=2, tasks=None,
                                                                                               trigger=None)])
        rows = job_runs.task_runs(NS(jobs=jobs), 42, now=NOW)
        self.assertEqual(jobs.calls, [{"job_id": 42, "expand_tasks": True,
                                       "start_time_from": int(NOW.timestamp() * 1000) - 7 * 86_400_000}])
        self.assertEqual(rows[0], {
            "task_run_id": 11, "job_run_id": 1, "task_key": "silver_movie", "attempt": 1, "run_trigger": "PERIODIC",
            "state": "FAILED", "state_message": "boom",
            "start_time": datetime(2026, 11, 26, 19, 0, tzinfo=timezone.utc),
            "end_time": datetime(2026, 11, 26, 19, 1, 30, tzinfo=timezone.utc), "duration_s": 90})
        self.assertEqual((rows[1]["state"], rows[1]["end_time"], rows[1]["duration_s"], rows[1]["state_message"]),
                         ("RUNNING", None, None, None))  # updated by a later run
        self.assertEqual(len(rows), 2)

    def test_job_id_must_be_a_number(self):
        sdk = NS(WorkspaceClient=mock.Mock())
        with mock.patch.dict(sys.modules, {"databricks": NS(sdk=sdk), "databricks.sdk": sdk}), \
                self.assertRaises(ValueError):
            job_runs.load(mock.Mock(), "{{job.id}}")


if __name__ == "__main__":
    unittest.main()
