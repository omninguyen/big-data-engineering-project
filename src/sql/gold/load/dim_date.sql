-- Full load of gold.dim_date (pipeline.py gold): rebuilt on every run, in one write (INSERT OVERWRITE), so the
-- calendar always reaches the end of next year.
INSERT OVERWRITE ${catalog}.gold.dim_date
  (date_key, date, year, quarter, month, month_name, day, day_of_week, day_name, is_weekend)
SELECT
  CAST(date_format(d, 'yyyyMMdd') AS INT)  AS date_key,
  d                                        AS date,
  year(d)                                  AS year,
  quarter(d)                               AS quarter,
  month(d)                                 AS month,
  date_format(d, 'MMMM')                   AS month_name,
  dayofmonth(d)                            AS day,
  weekday(d) + 1                           AS day_of_week,   -- weekday(): 0 = Monday ... 6 = Sunday
  date_format(d, 'EEEE')                   AS day_name,
  weekday(d) >= 5                          AS is_weekend
FROM (
  SELECT explode(sequence(DATE'1900-01-01', make_date(year(current_date()) + 1, 12, 31), INTERVAL 1 DAY)) AS d
);
