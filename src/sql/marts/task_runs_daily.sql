-- Mart view for the dashboard (Preset): runs, failures, retries and duration of every job task per day.
-- Days in Vietnam time (from_utc_timestamp, for the UTC session time zone of Databricks), as dt and batch_id.
-- Created or replaced by: pipeline.py marts.
CREATE OR REPLACE VIEW ${catalog}.marts.task_runs_daily (
  date            COMMENT 'day the task started (Vietnam time)',
  task_key        COMMENT 'task of the job, e.g. silver_movie',
  runs            COMMENT 'task attempts that day',
  succeeded       COMMENT 'attempts that ended with SUCCESS',
  failed          COMMENT 'attempts that ended with FAILED or TIMEDOUT',
  retries         COMMENT 'attempts after the first one (attempt > 0)',
  avg_duration_s  COMMENT 'average duration of the finished attempts, in seconds',
  max_duration_s  COMMENT 'longest finished attempt, in seconds'
)
COMMENT 'Runs, failures, retries and duration of the job tasks per day (ops.pipeline_task_runs)'
AS
SELECT
  to_date(from_utc_timestamp(start_time, 'Asia/Ho_Chi_Minh'))  AS date,
  task_key,
  count(*)                                                     AS runs,
  count_if(state = 'SUCCESS')                                  AS succeeded,
  count_if(state IN ('FAILED', 'TIMEDOUT'))                    AS failed,
  count_if(attempt > 0)                                        AS retries,
  CAST(round(avg(duration_s)) AS BIGINT)                       AS avg_duration_s,
  max(duration_s)                                              AS max_duration_s
FROM ${catalog}.ops.pipeline_task_runs
WHERE start_time IS NOT NULL
GROUP BY 1, task_key;
