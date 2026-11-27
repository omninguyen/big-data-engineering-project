-- Ops table of the job's task runs, from the Databricks Jobs API (src/job_runs.py): one row per task attempt.
-- Created if missing by: pipeline.py setup. Loaded by: pipeline.py ops --table pipeline_task_runs
-- (ops/load/pipeline_task_runs.sql).
CREATE TABLE IF NOT EXISTS ${catalog}.ops.pipeline_task_runs (
  task_run_id    BIGINT     NOT NULL COMMENT 'run id of one task attempt (Jobs API)',
  job_run_id     BIGINT              COMMENT 'run id of the whole job run',
  task_key       STRING              COMMENT 'task of databricks.yml, e.g. silver_movie',
  attempt        INT                 COMMENT '0 = first attempt, 1 = first retry, ...',
  run_trigger    STRING              COMMENT 'PERIODIC (schedule) | ONE_TIME (Run now) | RETRY (repair) ...',
  state          STRING              COMMENT 'SUCCESS | FAILED | TIMEDOUT | CANCELED | ..., RUNNING while going on',
  state_message  STRING              COMMENT 'message of the state, e.g. the error of a failed task',
  start_time     TIMESTAMP           COMMENT 'UTC',
  end_time       TIMESTAMP           COMMENT 'UTC, NULL while running',
  duration_s     BIGINT              COMMENT 'end_time - start_time, in seconds',
  _loaded_at     TIMESTAMP  NOT NULL COMMENT 'when this row was last written',
  CONSTRAINT pipeline_task_runs_pk PRIMARY KEY (task_run_id)
)
USING DELTA
LOCATION '${ops_root}/pipeline_task_runs'
COMMENT 'Task runs of the pipeline job (Databricks Jobs API), kept beyond the 60 days of the API';
