-- ============================================================
-- Query 1: Daily Traffic Summary (HiveQL)
-- For each (log_date, status_code): request_count, total_bytes
-- ============================================================

INSERT OVERWRITE LOCAL DIRECTORY '${hivevar:OUTPUT_DIR}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT
    log_date,
    status_code,
    COUNT(*)              AS request_count,
    SUM(bytes_transferred) AS total_bytes
FROM nasa_logs
GROUP BY log_date, status_code
ORDER BY log_date ASC, status_code ASC;
