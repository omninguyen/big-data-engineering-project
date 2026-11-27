-- Load of ops.pipeline_task_runs (pipeline.py ops --table pipeline_task_runs), in one write (INSERT OVERWRITE): the
-- task runs of the last days, read from the Jobs API into the temp view job_task_runs (src/job_runs.py), replace
-- their earlier rows; older runs are kept (the Jobs API forgets them after 60 days).
INSERT OVERWRITE ${catalog}.ops.pipeline_task_runs
  (task_run_id, job_run_id, task_key, attempt, run_trigger, state, state_message, start_time, end_time, duration_s,
   _loaded_at)
SELECT
  task_run_id, job_run_id, task_key, attempt, run_trigger, state, state_message, start_time, end_time, duration_s,
  _loaded_at
FROM (
  SELECT *, row_number() OVER (PARTITION BY task_run_id ORDER BY _loaded_at DESC) AS rn
  FROM (
    SELECT
      task_run_id, job_run_id, task_key, attempt, run_trigger, state, state_message, start_time, end_time,
      duration_s, current_timestamp() AS _loaded_at
    FROM job_task_runs
    WHERE task_run_id IS NOT NULL
    UNION ALL
    SELECT
      task_run_id, job_run_id, task_key, attempt, run_trigger, state, state_message, start_time, end_time,
      duration_s, _loaded_at
    FROM ${catalog}.ops.pipeline_task_runs
  )
)
WHERE rn = 1;
