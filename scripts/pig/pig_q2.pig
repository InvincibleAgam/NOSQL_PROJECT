-- ============================================================
-- Query 2: Top 20 Requested Resources (Pig Latin)
-- For each resource_path: request_count, total_bytes, distinct_host_count
-- ============================================================

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

-- Group by resource_path
grouped = GROUP raw BY resource_path;

-- Aggregate: count requests, sum bytes, count distinct hosts
agg = FOREACH grouped {
    unique_hosts = DISTINCT raw.host;
    GENERATE
        group                       AS resource_path,
        COUNT(raw)                  AS request_count,
        SUM(raw.bytes_transferred)  AS total_bytes,
        COUNT(unique_hosts)         AS distinct_host_count;
};

-- Sort by request_count DESC, then take top 20
sorted = ORDER agg BY request_count DESC;
top20 = LIMIT sorted 20;

-- Write output
STORE top20 INTO '$OUTPUT' USING PigStorage('\t');
