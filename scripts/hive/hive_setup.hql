-- ============================================================
-- Hive Setup: Create external table over staged TSV input
-- ============================================================

DROP TABLE IF EXISTS nasa_logs;

CREATE EXTERNAL TABLE nasa_logs (
    host              STRING,
    ts                STRING,
    log_date          STRING,
    log_hour          INT,
    http_method       STRING,
    resource_path     STRING,
    protocol_version  STRING,
    status_code       INT,
    bytes_transferred BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hivevar:INPUT_DIR}';
