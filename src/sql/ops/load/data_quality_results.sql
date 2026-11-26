-- Full load of ops.data_quality_results (pipeline.py ops): rebuilt on every run, in one write (INSERT OVERWRITE),
-- from ops.pipeline_record_metrics, three checks per batch:
--   rejected_rate     rows left out of silver; critical for TMDB and MusicBrainz (every row has an id), a warning
--                     for ListenBrainz, where most of them are albums ListenBrainz has no listens for
--   duplicate_rate    valid rows repeating a key of the same batch (a movie both popular and top rated), kept once
--   loaded_to_silver  the batch reached its silver table
WITH checks AS (
  SELECT
    dataset, batch_id, 'rejected_rate' AS check_name,
    CASE WHEN dataset = 'listenbrainz_release_group_popularity' THEN 'warning' ELSE 'critical' END  AS severity,
    try_divide(rows_rejected, rows_read)                                                          AS value,
    '<=' AS op,
    CASE WHEN dataset = 'listenbrainz_release_group_popularity' THEN 0.5D ELSE 0.0D END           AS threshold,
    CASE WHEN dataset = 'listenbrainz_release_group_popularity'
      THEN 'share of rows left out of silver: albums without ListenBrainz listens, or without id'
      ELSE 'share of rows left out of silver: rows without a valid id' END                       AS description
  FROM ${catalog}.ops.pipeline_record_metrics
  UNION ALL
  SELECT
    dataset, batch_id, 'duplicate_rate', 'warning', coalesce(try_divide(rows_duplicate, rows_valid), 0.0D), '<=',
    0.5D, 'share of valid rows repeating a key of the same batch, kept once in silver'
  FROM ${catalog}.ops.pipeline_record_metrics
  UNION ALL
  SELECT
    dataset, batch_id, 'loaded_to_silver', 'critical', CASE WHEN silver_loaded_at IS NULL THEN 0.0D ELSE 1.0D END,
    '>=', 1.0D, 'the batch reached its silver table (silver.load_log)'
  FROM ${catalog}.ops.pipeline_record_metrics
)
INSERT OVERWRITE ${catalog}.ops.data_quality_results
  (dataset, batch_id, check_name, severity, value, op, threshold, passed, description)
SELECT
  dataset, batch_id, check_name, severity, value, op, threshold,
  CASE op WHEN '<=' THEN value <= threshold ELSE value >= threshold END  AS passed,
  description
FROM checks;
