-- ============================================================
-- NASA HTTP Log Analysis — MySQL Schema
-- One table per query, each with pipeline metadata columns.
-- ============================================================

CREATE DATABASE IF NOT EXISTS nasa_log_analysis;
USE nasa_log_analysis;

-- ── Query 1: Daily Traffic Summary ─────────────────────────
-- For each (log_date, status_code): request_count, total_bytes
CREATE TABLE IF NOT EXISTS query1_results (
    id                INT AUTO_INCREMENT PRIMARY KEY,
    pipeline_name     VARCHAR(20)   NOT NULL,
    run_id            VARCHAR(50)   NOT NULL,
    batch_id          INT           NOT NULL,
    execution_time    DATETIME      NOT NULL,
    log_date          DATE          NOT NULL,
    status_code       INT           NOT NULL,
    request_count     INT           NOT NULL,
    total_bytes       BIGINT        NOT NULL,
    INDEX idx_q1_run  (run_id),
    INDEX idx_q1_pipe (pipeline_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── Query 2: Top Requested Resources ───────────────────────
-- Top 20 resource_paths by request_count
CREATE TABLE IF NOT EXISTS query2_results (
    id                  INT AUTO_INCREMENT PRIMARY KEY,
    pipeline_name       VARCHAR(20)   NOT NULL,
    run_id              VARCHAR(50)   NOT NULL,
    batch_id            INT           NOT NULL,
    execution_time      DATETIME      NOT NULL,
    resource_path       VARCHAR(512)  NOT NULL,
    request_count       INT           NOT NULL,
    total_bytes         BIGINT        NOT NULL,
    distinct_host_count INT           NOT NULL,
    INDEX idx_q2_run    (run_id),
    INDEX idx_q2_pipe   (pipeline_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── Query 3: Hourly Error Analysis ─────────────────────────
-- For each (log_date, log_hour): error counts, total counts, error_rate, distinct error hosts
CREATE TABLE IF NOT EXISTS query3_results (
    id                    INT AUTO_INCREMENT PRIMARY KEY,
    pipeline_name         VARCHAR(20)   NOT NULL,
    run_id                VARCHAR(50)   NOT NULL,
    batch_id              INT           NOT NULL,
    execution_time        DATETIME      NOT NULL,
    log_date              DATE          NOT NULL,
    log_hour              INT           NOT NULL,
    error_request_count   INT           NOT NULL,
    total_request_count   INT           NOT NULL,
    error_rate            DECIMAL(10,6) NOT NULL,
    distinct_error_hosts  INT           NOT NULL,
    INDEX idx_q3_run      (run_id),
    INDEX idx_q3_pipe     (pipeline_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── Run Metadata Table ─────────────────────────────────────
-- Stores per-run summary information
CREATE TABLE IF NOT EXISTS run_metadata (
    id                INT AUTO_INCREMENT PRIMARY KEY,
    run_id            VARCHAR(50)   NOT NULL UNIQUE,
    pipeline_name     VARCHAR(20)   NOT NULL,
    batch_strategy    VARCHAR(20)   NOT NULL DEFAULT 'fixed',
    batch_size        INT           NOT NULL,
    total_records     INT           NOT NULL,
    malformed_records INT           NOT NULL,
    num_batches       INT           NOT NULL,
    avg_batch_size    DECIMAL(10,2) NOT NULL,
    runtime_seconds   DECIMAL(10,3) NOT NULL,
    started_at        DATETIME      NOT NULL,
    completed_at      DATETIME      NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── Batch Metadata Table (NEW) ─────────────────────────────
-- Stores per-batch details: records processed, malformed count,
-- batch key (e.g. "1995-07" for monthly, "1995-W27" for weekly, "batch_42" for fixed)
CREATE TABLE IF NOT EXISTS batch_metadata (
    id                  INT AUTO_INCREMENT PRIMARY KEY,
    run_id              VARCHAR(50)   NOT NULL,
    pipeline_name       VARCHAR(20)   NOT NULL,
    batch_sequence      INT           NOT NULL,
    batch_key           VARCHAR(50)   NOT NULL,
    batch_strategy      VARCHAR(20)   NOT NULL DEFAULT 'fixed',
    total_records       INT           NOT NULL,
    valid_records       INT           NOT NULL,
    malformed_records   INT           NOT NULL,
    started_at          DATETIME      NOT NULL,
    completed_at        DATETIME      NOT NULL,
    INDEX idx_bm_run    (run_id),
    INDEX idx_bm_pipe   (pipeline_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
