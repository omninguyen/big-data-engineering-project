"""Task runs of this job from the Databricks Jobs API -> ops.pipeline_task_runs (pipeline.py ops --table
pipeline_task_runs --job-id <id>).

The Jobs API (runs/list) only needs the job's own permissions: the run-as identity of the job can view its runs.
The system tables (system.lakeflow) would need a grant from a metastore admin. Databricks keeps runs for 60 days;
the table keeps them for good (src/sql/ops/load/pipeline_task_runs.sql). Runs started in the last DAYS days are read
again, so a run still going on at the end of today's job gets its final state on the next run.
"""
import logging
from datetime import datetime, timedelta, timezone

log = logging.getLogger(__name__)
DAYS = 7
VIEW = "job_task_runs"  # temp view read by src/sql/ops/load/pipeline_task_runs.sql
SCHEMA = ("task_run_id BIGINT, job_run_id BIGINT, task_key STRING, attempt INT, run_trigger STRING, state STRING, "
          "state_message STRING, start_time TIMESTAMP, end_time TIMESTAMP, duration_s BIGINT")


def _time(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc) if ms else None


def _name(value):
    """An SDK enum by its name (SUCCESS), a plain value as is."""
    return getattr(value, "value", value)


def task_runs(client, job_id: int, now=None) -> list:
    """One row per task attempt of the job runs started in the last DAYS days."""
    now = now or datetime.now(timezone.utc)
    since = int((now - timedelta(days=DAYS)).timestamp() * 1000)
    rows = []
    for run in client.jobs.list_runs(job_id=job_id, expand_tasks=True, start_time_from=since):
        for t in run.tasks or []:
            state = t.state
            rows.append({
                "task_run_id": t.run_id, "job_run_id": run.run_id, "task_key": t.task_key,
                "attempt": t.attempt_number or 0, "run_trigger": _name(run.trigger),
                "state": _name(state.result_state or state.life_cycle_state) if state else None,
                "state_message": (state.state_message or None) if state else None,
                "start_time": _time(t.start_time), "end_time": _time(t.end_time),
                "duration_s": (t.end_time - t.start_time) // 1000 if t.start_time and t.end_time else None})
    return rows


def load(spark, job_id: str) -> dict:
    """Read the task runs into the temp view, then write ops.pipeline_task_runs. Returns {"rows": n}."""
    import sql
    from databricks.sdk import WorkspaceClient  # authenticates as the job's run-as identity on Databricks
    if not str(job_id).isdigit():
        raise ValueError(f"--job-id must be the numeric id of the job ({{{{job.id}}}} in databricks.yml), "
                         f"got {job_id!r}")
    rows = task_runs(WorkspaceClient(), int(job_id))
    spark.createDataFrame(rows, SCHEMA).createOrReplaceTempView(VIEW)
    log.info("Jobs API: %s task runs of job %s in the last %s days", len(rows), job_id, DAYS)
    return sql.load_ops_job_table(spark, "pipeline_task_runs")
