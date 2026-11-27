-- Mart view for the dashboard (Preset): the tables and views of every layer, from Unity Catalog's information schema.
-- information_schema only lists what the querying principal may see: grant the dashboard's principal BROWSE on the
-- catalog (metadata only) to list every layer. Times are shown in Vietnam time.
-- Created or replaced by: pipeline.py marts.
CREATE OR REPLACE VIEW ${catalog}.marts.data_catalog (
  layer         COMMENT 'schema: bronze | silver | gold | ops | marts',
  table_name    COMMENT 'table or view',
  table_type    COMMENT 'EXTERNAL | MANAGED | VIEW',
  description   COMMENT 'comment of the table',
  owner         COMMENT 'owner of the table',
  created       COMMENT 'when it was created (Vietnam time)',
  last_altered  COMMENT 'when it was last changed (Vietnam time)'
)
COMMENT 'Tables and views of every layer (information_schema.tables)'
AS
SELECT
  table_schema                                          AS layer,
  table_name,
  table_type,
  comment                                               AS description,
  table_owner                                           AS owner,
  from_utc_timestamp(created, 'Asia/Ho_Chi_Minh')       AS created,
  from_utc_timestamp(last_altered, 'Asia/Ho_Chi_Minh')  AS last_altered
FROM ${catalog}.information_schema.tables
WHERE table_schema IN ('bronze', 'silver', 'gold', 'ops', 'marts');
