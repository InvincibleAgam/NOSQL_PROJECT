-- ============================================================
-- Query 3: Hourly Error Analysis (Pig Latin)
-- For each (log_date, log_hour): error counts, total counts,
-- error_rate, distinct error hosts
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

-- Group by (log_date, log_hour)
grouped = GROUP raw BY (log_date, log_hour);

-- Aggregate with error filtering
results = FOREACH grouped {
    -- Filter to error records (status 400-599)
    errors = FILTER raw BY (status_code >= 400 AND status_code <= 599);
    distinct_error_hosts = DISTINCT errors.host;
    GENERATE
        group.log_date              AS log_date,
        group.log_hour              AS log_hour,
        COUNT(errors)               AS error_request_count,
        COUNT(raw)                  AS total_request_count,
        (double)COUNT(errors) / (double)COUNT(raw) AS error_rate,
        COUNT(distinct_error_hosts) AS distinct_error_hosts;
};

-- Sort for deterministic output
sorted = ORDER results BY log_date ASC, log_hour ASC;

-- Write output
STORE sorted INTO '$OUTPUT' USING PigStorage('\t');
