-- Gold dimension of calendar days, one row per day from 1900-01-01 to the end of next year.
-- Created if missing by: pipeline.py setup. Rebuilt on every run (gold/load/dim_date.sql).
CREATE TABLE IF NOT EXISTS ${catalog}.gold.dim_date (
  date_key     INT      NOT NULL COMMENT 'yyyymmdd, e.g. 20261126',
  date         DATE     NOT NULL,
  year         INT      NOT NULL,
  quarter      INT      NOT NULL COMMENT '1 to 4',
  month        INT      NOT NULL COMMENT '1 to 12',
  month_name   STRING   NOT NULL COMMENT 'January ... December',
  day          INT      NOT NULL COMMENT 'day of the month',
  day_of_week  INT      NOT NULL COMMENT '1 = Monday ... 7 = Sunday',
  day_name     STRING   NOT NULL COMMENT 'Monday ... Sunday',
  is_weekend   BOOLEAN  NOT NULL COMMENT 'Saturday or Sunday',
  CONSTRAINT dim_date_pk PRIMARY KEY (date_key)
)
USING DELTA
LOCATION '${gold_root}/dim_date'
COMMENT 'Calendar days from 1900-01-01 to the end of next year, full load';
