-- =============================================================================
-- Part 4 - Make the Iceberg tables in S3 readable from Snowflake
-- Run in Snowsight (Snowflake free trial) as ACCOUNTADMIN, top to bottom.
-- Replace <BUCKET> and <ACCOUNT_ID> first.
-- =============================================================================
USE ROLE ACCOUNTADMIN;

-- 1. Compute + a place for the tables -------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS TAXI_WH
  WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;
CREATE DATABASE IF NOT EXISTS TAXI_DB;
CREATE SCHEMA IF NOT EXISTS TAXI_DB.ANALYTICS;
USE WAREHOUSE TAXI_WH;
USE SCHEMA TAXI_DB.ANALYTICS;

-- 2. External volume = "how Snowflake reaches s3://<BUCKET>/warehouse/" ------------
--    Read-only (ALLOW_WRITES = FALSE): Spark owns these tables, Snowflake only reads.
CREATE EXTERNAL VOLUME IF NOT EXISTS taxi_s3_vol
  STORAGE_LOCATIONS = ((
    NAME = 'taxi-s3-warehouse'
    STORAGE_PROVIDER = 'S3'
    STORAGE_BASE_URL = 's3://<BUCKET>/warehouse/'
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<ACCOUNT_ID>:role/taxi-snowflake-read-role'
    STORAGE_AWS_EXTERNAL_ID = 'taxi_iceberg_ext_id'
  ))
  ALLOW_WRITES = FALSE;

-- 3. Get Snowflake's IAM user ARN and put it in the AWS role's trust policy:
DESC EXTERNAL VOLUME taxi_s3_vol;
--    In the STORAGE_LOCATION_1 row, copy STORAGE_AWS_IAM_USER_ARN, then in your terminal:
--      SNOWFLAKE_IAM_USER_ARN=<that arn> SNOWFLAKE_EXTERNAL_ID=taxi_iceberg_ext_id bash infra/setup_aws.sh
--    Wait ~30 seconds, then verify:
SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('taxi_s3_vol');   -- expect "success": true for read/list

-- 4. Catalog integration: read Iceberg metadata.json files straight from object storage
CREATE CATALOG INTEGRATION IF NOT EXISTS taxi_iceberg_files
  CATALOG_SOURCE = OBJECT_STORE
  TABLE_FORMAT = ICEBERG
  ENABLED = TRUE;

-- 5. Create the tables.
--    Run `make snowflake-sql` (python -m pipeline.snowflake_metadata) and paste the
--    generated build/snowflake_tables.sql here. It looks like:
--
-- CREATE ICEBERG TABLE IF NOT EXISTS trips_clean
--   EXTERNAL_VOLUME = 'taxi_s3_vol'
--   CATALOG = 'taxi_iceberg_files'
--   METADATA_FILE_PATH = 'nyc_taxi/trips_clean/metadata/00004-....metadata.json';
--
--    After every new pipeline run, run the generated ALTER ICEBERG TABLE ... REFRESH lines.

-- 6. Smoke test
SHOW ICEBERG TABLES;
SELECT COUNT(*) FROM trips_clean;
SELECT COUNT(*) FROM daily_zone_metrics;


-- =============================================================================
-- ALTERNATIVES (use instead of step 4/5 if you chose a different catalog)
-- =============================================================================
-- A) AWS Glue Data Catalog (CATALOG_TYPE=glue). Needs an IAM role Snowflake can
--    assume with glue:GetTable/GetDatabase permissions (see Snowflake docs).
-- CREATE CATALOG INTEGRATION taxi_glue_cat
--   CATALOG_SOURCE = GLUE
--   CATALOG_NAMESPACE = 'nyc_taxi'
--   TABLE_FORMAT = ICEBERG
--   GLUE_AWS_ROLE_ARN = 'arn:aws:iam::<ACCOUNT_ID>:role/<snowflake-glue-role>'
--   GLUE_CATALOG_ID = '<ACCOUNT_ID>'
--   GLUE_REGION = '<REGION>'
--   ENABLED = TRUE;
-- CREATE ICEBERG TABLE trips_clean
--   EXTERNAL_VOLUME = 'taxi_s3_vol' CATALOG = 'taxi_glue_cat' CATALOG_TABLE_NAME = 'trips_clean'
--   AUTO_REFRESH = TRUE;
--
-- B) Snowflake Open Catalog (managed Polaris, reachable from Snowflake over the internet):
-- CREATE CATALOG INTEGRATION taxi_open_catalog
--   CATALOG_SOURCE = POLARIS
--   TABLE_FORMAT = ICEBERG
--   CATALOG_NAMESPACE = 'nyc_taxi'
--   REST_CONFIG = (CATALOG_URI = 'https://<org>-<open_catalog_account>.snowflakecomputing.com/polaris/api/catalog'
--                  CATALOG_NAME = 'taxi_catalog')
--   REST_AUTHENTICATION = (TYPE = OAUTH OAUTH_CLIENT_ID = '<id>' OAUTH_CLIENT_SECRET = '<secret>'
--                          OAUTH_ALLOWED_SCOPES = ('PRINCIPAL_ROLE:ALL'))
--   ENABLED = TRUE;
-- CREATE ICEBERG TABLE trips_clean
--   EXTERNAL_VOLUME = 'taxi_s3_vol' CATALOG = 'taxi_open_catalog' CATALOG_TABLE_NAME = 'trips_clean'
--   AUTO_REFRESH = TRUE;
