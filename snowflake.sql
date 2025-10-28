-- 1) Create Storage Integration (points Snowflake to your S3 via your IAM role)
CREATE OR REPLACE STORAGE INTEGRATION aws_integration_data_pipeline
  TYPE = EXTERNAL_STAGE
  ENABLED = TRUE
  STORAGE_PROVIDER = S3
  STORAGE_ALLOWED_LOCATIONS = ('s3://tuf-narayan-data-bucket/')
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::539524425104:role/role1';

-- 2) Describe Integration (copy Snowflake IAM ARN + External ID for AWS trust policy)
DESC STORAGE INTEGRATION aws_integration_data_pipeline;

-- 3) Use your DB/Schema
USE DATABASE SNOWFLAKE_LEARNING_DB;
USE SCHEMA PUBLIC;

-- 4) Create Stage bound to the integration (your bucket)
CREATE OR REPLACE STAGE aws_stage_data_pipeline
  URL = 's3://tuf-narayan-data-bucket/'
  STORAGE_INTEGRATION = aws_integration_data_pipeline;

-- 5) Verify Stage (list S3 objects visible to Snowflake via integration)
LIST @aws_stage_data_pipeline;







-- Use DB & Schema
USE DATABASE SNOWFLAKE_LEARNING_DB;
USE SCHEMA PUBLIC;

-- File format for JSON array
CREATE OR REPLACE FILE FORMAT ff_json_array TYPE = JSON STRIP_OUTER_ARRAY = TRUE;

-- Raw table
CREATE OR REPLACE TABLE raw_store_json (
  rec VARIANT,
  src_filename STRING,
  file_row_number INT,
  load_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Load JSON from S3 stage
COPY INTO raw_store_json (rec, src_filename, file_row_number)
FROM (
  SELECT $1, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER
  FROM @aws_stage_data_pipeline/store_products.json (FILE_FORMAT => ff_json_array)
)
ON_ERROR = 'CONTINUE';

-- Clean view (wide format)
CREATE OR REPLACE VIEW vw_store_clean AS
SELECT
  LPAD(TRIM(rec:" 08075"::string), 5, '0') AS zip_new,
  UPPER(TRIM(rec:" NJ"::string)) AS state_new,
  TRIM(INITCAP(rec:"120 jefferson st."::string)) AS address_new,
  TRIM(INITCAP(rec:"Doe"::string)) AS last_name_new,
  TRIM(INITCAP(rec:"John"::string)) AS first_name_new,
  TRIM(INITCAP(rec:"Riverside"::string)) AS city_new,
  src_filename, file_row_number, load_ts
FROM raw_store_json;


CREATE OR REPLACE VIEW vw_store_simple AS
SELECT
  LPAD(TRIM(rec:" 08075"::string), 5, '0') AS zip_new,
  UPPER(TRIM(rec:" NJ"::string))           AS state_new,
  TRIM(rec:"120 jefferson st."::string)    AS address_new,
  TRIM(rec:"Doe"::string)                  AS last_name_new,
  TRIM(rec:"John"::string)                 AS first_name_new,
  TRIM(rec:"Riverside"::string)            AS city_new
FROM raw_store_json;


SELECT *
FROM vw_store_simple
LIMIT 10;





-- File format for JSON array
CREATE OR REPLACE FILE FORMAT ff_json_array TYPE = JSON STRIP_OUTER_ARRAY = TRUE;

-- Raw table for TFL Tube Status
CREATE OR REPLACE TABLE raw_tfl_tube_status (
  rec VARIANT,
  src_filename STRING,
  file_row_number INT,
  load_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


COPY INTO raw_tfl_tube_status (rec, src_filename, file_row_number)
FROM (
  SELECT $1, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER
  FROM @aws_stage_data_pipeline/tube_status.json (FILE_FORMAT => ff_json_array)
)
ON_ERROR = 'CONTINUE';


CREATE OR REPLACE VIEW vw_tfl_tube_status_simple AS
SELECT
  rec:id::string                                  AS line_id,
  rec:name::string                                AS line_name,
  rec:modeName::string                            AS mode_name,
  rec:lineStatuses[0].statusSeverity::int         AS status_severity,
  rec:lineStatuses[0].statusSeverityDescription::string AS status_desc,
  rec:lineStatuses[0].reason::string              AS reason,
  TRY_TO_TIMESTAMP_NTZ(rec:lineStatuses[0].validityPeriods[0].fromDate::string) AS from_utc,
  TRY_TO_TIMESTAMP_NTZ(rec:lineStatuses[0].validityPeriods[0].toDate::string)   AS to_utc,
  COALESCE(rec:lineStatuses[0].validityPeriods[0].isNow::boolean, FALSE)        AS is_now
FROM raw_tfl_tube_status;







SELECT * FROM vw_tfl_tube_status_simple LIMIT 10;
