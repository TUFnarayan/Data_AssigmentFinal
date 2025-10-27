# End-to-End Pipeline Overview
What It Does

1.Fetches data from public APIs (JSON & CSV).
2.Converts everything to JSON and uploads to S3.
3.Snowflake automatically ingests from S3 and creates simple views for querying.


# AWS Lambda (Python)
Uses requests to download:

1.TFL Tube Status → JSON
2.Sample Addresses → CSV → converted to JSON

Uploads to S3 bucket: tuf-narayan-data-bucket using boto3.
Code snippet:
sources = [
  {"format": "json", "url": "https://api.tfl.gov.uk/Line/Mode/tube/Status", "path": "tube_status"},
  {"format": "csv", "url": "https://people.sc.fsu.edu/~jburkardt/data/csv/addresses.csv", "path": "store_products"}
]
for src in sources:
    fetch_and_store(src["format"], src["url"], src["path"])


# Snowflake Setup
File Format
STRIP_OUTER_ARRAY=TRUE so each JSON element becomes a row.

1.Raw Tables
CREATE TABLE raw_store_json (
  rec VARIANT, src_filename STRING, file_row_number INT,
  load_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE raw_tfl_tube_status (
  rec VARIANT, src_filename STRING, file_row_number INT,
  load_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

2.Load from S3
COPY INTO raw_store_json
FROM (
  SELECT $1, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER
  FROM @aws_stage_data_pipeline/store_products.json (FILE_FORMAT => ff_json_array)
);

COPY INTO raw_tfl_tube_status
FROM (
  SELECT $1, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER
  FROM @aws_stage_data_pipeline/tube_status.json (FILE_FORMAT => ff_json_array)
);


# Views
Store Products

CREATE VIEW vw_store_simple AS
SELECT
  LPAD(TRIM(rec:" 08075"::string),5,'0') AS zip_new,
  UPPER(TRIM(rec:" NJ"::string)) AS state_new,
  TRIM(rec:"120 jefferson st."::string) AS address_new,
  TRIM(rec:"Doe"::string) AS last_name_new,
  TRIM(rec:"John"::string) AS first_name_new,
  TRIM(rec:"Riverside"::string) AS city_new
FROM raw_store_json;

tube status

CREATE VIEW vw_tfl_tube_status_simple AS
SELECT
  rec:id::string AS line_id,
  rec:name::string AS line_name,
  rec:modeName::string AS mode_name,
  rec:lineStatuses[0].statusSeverity::int AS status_severity,
  rec:lineStatuses[0].statusSeverityDescription::string AS status_desc
FROM raw_tfl_tube_status;

# quick test
1.SELECT * FROM vw_store_simple LIMIT 10;
2.SELECT * FROM vw_tfl_tube_status_simple LIMIT 10;


