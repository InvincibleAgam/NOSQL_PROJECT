-- ============================================================
-- Query 3: Hourly Error Analysis (HiveQL)
-- For each (log_date, log_hour): error counts, total counts,
-- error_rate, distinct error hosts
-- ============================================================

INSERT OVERWRITE LOCAL DIRECTORY '${hivevar:OUTPUT_DIR}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT
    log_date,
    log_hour,
    SUM(CASE WHEN status_code BETWEEN 400 AND 599 THEN 1 ELSE 0 END)  AS error_request_count,
    COUNT(*)                                                            AS total_request_count,
    SUM(CASE WHEN status_code BETWEEN 400 AND 599 THEN 1 ELSE 0 END)
        / COUNT(*)                                                      AS error_rate,
    COUNT(DISTINCT CASE WHEN status_code BETWEEN 400 AND 599 THEN host ELSE NULL END)
                                                                        AS distinct_error_hosts
FROM nasa_logs
GROUP BY log_date, log_hour
ORDER BY log_date ASC, log_hour ASC;
