-- ============================================================
-- Query 2: Top 20 Requested Resources (HiveQL)
-- For each resource_path: request_count, total_bytes, distinct_host_count
-- ============================================================

INSERT OVERWRITE LOCAL DIRECTORY '${hivevar:OUTPUT_DIR}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT
    resource_path,
    request_count,
    total_bytes,
    distinct_host_count
FROM (
    SELECT
        resource_path,
        COUNT(*)                    AS request_count,
        SUM(bytes_transferred)      AS total_bytes,
        COUNT(DISTINCT host)        AS distinct_host_count
    FROM nasa_logs
    GROUP BY resource_path
) sub
ORDER BY request_count DESC
LIMIT 20;
