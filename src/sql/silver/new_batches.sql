-- Bronze batches that silver.${table} has not loaded yet, oldest first (pipeline.py silver).
SELECT batch_id
FROM (
  SELECT DISTINCT batch_id FROM ${catalog}.bronze.${source} WHERE batch_id <> ''
  EXCEPT
  SELECT batch_id FROM ${catalog}.silver.load_log WHERE table_name = '${table}'
)
ORDER BY batch_id;
