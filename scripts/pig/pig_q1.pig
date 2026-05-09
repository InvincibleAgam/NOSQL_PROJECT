-- ============================================================
-- Query 1: Daily Traffic Summary (Pig Latin)
-- For each (log_date, status_code): request_count, total_bytes
-- ============================================================

-- Input: TSV with columns:
-- host(0) timestamp(1) log_date(2) log_hour(3) http_method(4)
-- resource_path(5) protocol_version(6) status_code(7) bytes_transferred(8)

raw = LOAD '$INPUT' USING PigStorage('\t') AS (
    host:chararray,
    ts:chararray,
    log_date:chararray,
    log_hour:int,
    http_method:chararray,
    resource_path:chararray,
    protocol_version:chararray,
    status_code:int,
    bytes_transferred:long
);

-- Group by (log_date, status_code)
grouped = GROUP raw BY (log_date, status_code);

-- Aggregate: count requests, sum bytes
results = FOREACH grouped GENERATE
    group.log_date      AS log_date,
    group.status_code   AS status_code,
    COUNT(raw)          AS request_count,
    SUM(raw.bytes_transferred) AS total_bytes;

-- Sort for deterministic output
sorted = ORDER results BY log_date ASC, status_code ASC;

-- Write output
STORE sorted INTO '$OUTPUT' USING PigStorage('\t');
