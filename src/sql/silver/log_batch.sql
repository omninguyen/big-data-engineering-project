-- Records that silver.${table} has loaded bronze batch ${batch_id} (pipeline.py silver).
INSERT INTO ${catalog}.silver.load_log (table_name, batch_id, rows_inserted, rows_updated, loaded_at)
VALUES ('${table}', '${batch_id}', ${inserted}, ${updated}, current_timestamp());
