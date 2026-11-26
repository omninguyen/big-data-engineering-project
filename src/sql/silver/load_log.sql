-- Bronze batches loaded into each incremental silver table (pipeline.py silver): the watermark of the incremental
-- loads (a batch listed here is not loaded again), and a monitoring view of how far every batch got.
CREATE TABLE IF NOT EXISTS ${catalog}.silver.load_log (
  table_name     STRING     NOT NULL COMMENT 'silver table, e.g. movie',
  batch_id       STRING     NOT NULL COMMENT 'bronze batch loaded into it',
  rows_inserted  BIGINT     NOT NULL,
  rows_updated   BIGINT     NOT NULL COMMENT 'SCD2: versions closed; daily metrics: rows replaced by a later batch',
  loaded_at      TIMESTAMP  NOT NULL,
  CONSTRAINT load_log_pk PRIMARY KEY (table_name, batch_id)
)
USING DELTA
LOCATION '${silver_root}/load_log'
COMMENT 'Bronze batches loaded into each incremental silver table';
