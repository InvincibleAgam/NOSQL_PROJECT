package com.invincibleagam.core;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

public class DatabaseManager {
    private static Connection getConnection(boolean useDatabase) throws Exception {
        String url = "jdbc:mysql://" + Config.MYSQL_HOST + ":" + Config.MYSQL_PORT + "/";
        if (useDatabase) {
            url += Config.MYSQL_DATABASE;
        }
        return DriverManager.getConnection(url, Config.MYSQL_USER, Config.MYSQL_PASSWORD);
    }

    public static void createDatabaseAndTables() {
        try {
            // Create database
            try (Connection conn = getConnection(false); Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS " + Config.MYSQL_DATABASE);
            }

            // Create tables
            try (Connection conn = getConnection(true); Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("CREATE TABLE IF NOT EXISTS run_metadata (" +
                        "run_id VARCHAR(100) PRIMARY KEY, " +
                        "pipeline_name VARCHAR(50), " +
                        "batch_strategy VARCHAR(20) DEFAULT 'fixed', " +
                        "batch_size INT, " +
                        "total_records INT, " +
                        "malformed_records INT, " +
                        "num_batches INT, " +
                        "avg_batch_size FLOAT, " +
                        "runtime_seconds FLOAT, " +
                        "started_at DATETIME, " +
                        "completed_at DATETIME)");

                stmt.executeUpdate("CREATE TABLE IF NOT EXISTS batch_metadata (" +
                        "id INT AUTO_INCREMENT PRIMARY KEY, " +
                        "run_id VARCHAR(100), " +
                        "pipeline_name VARCHAR(50), " +
                        "batch_sequence INT, " +
                        "batch_key VARCHAR(50), " +
                        "batch_strategy VARCHAR(20) DEFAULT 'fixed', " +
                        "total_records INT, " +
                        "valid_records INT, " +
                        "malformed_records INT, " +
                        "started_at DATETIME, " +
                        "completed_at DATETIME)");

                stmt.executeUpdate("CREATE TABLE IF NOT EXISTS query1_results (" +
                        "id INT AUTO_INCREMENT PRIMARY KEY, " +
                        "pipeline_name VARCHAR(50), " +
                        "run_id VARCHAR(100), " +
                        "batch_id INT, " +
                        "execution_time DATETIME, " +
                        "log_date DATE, " +
                        "status_code INT, " +
                        "request_count INT, " +
                        "total_bytes BIGINT)");

                stmt.executeUpdate("CREATE TABLE IF NOT EXISTS query2_results (" +
                        "id INT AUTO_INCREMENT PRIMARY KEY, " +
                        "pipeline_name VARCHAR(50), " +
                        "run_id VARCHAR(100), " +
                        "batch_id INT, " +
                        "execution_time DATETIME, " +
                        "resource_path VARCHAR(512), " +
                        "request_count INT, " +
                        "total_bytes BIGINT, " +
                        "distinct_host_count INT)");

                stmt.executeUpdate("CREATE TABLE IF NOT EXISTS query3_results (" +
                        "id INT AUTO_INCREMENT PRIMARY KEY, " +
                        "pipeline_name VARCHAR(50), " +
                        "run_id VARCHAR(100), " +
                        "batch_id INT, " +
                        "execution_time DATETIME, " +
                        "log_date DATE, " +
                        "log_hour INT, " +
                        "error_request_count INT, " +
                        "total_request_count INT, " +
                        "error_rate FLOAT, " +
                        "distinct_error_hosts INT)");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void loadRunMetadata(String runId, String pipelineName, String batchStrategy,
                                       int batchSize, int totalRecords,
                                       int malformedRecords, int numBatches, float avgBatchSize,
                                       float runtimeSeconds, String startedAt, String completedAt) {
        String sql = "INSERT INTO run_metadata (run_id, pipeline_name, batch_strategy, batch_size, total_records, " +
                     "malformed_records, num_batches, avg_batch_size, runtime_seconds, started_at, completed_at) " +
                     "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (Connection conn = getConnection(true); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, runId);
            ps.setString(2, pipelineName);
            ps.setString(3, batchStrategy);
            ps.setInt(4, batchSize);
            ps.setInt(5, totalRecords);
            ps.setInt(6, malformedRecords);
            ps.setInt(7, numBatches);
            ps.setFloat(8, avgBatchSize);
            ps.setFloat(9, runtimeSeconds);
            ps.setString(10, startedAt);
            ps.setString(11, completedAt);
            ps.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Records per-batch metadata into the batch_metadata table.
     */
    public static void loadBatchMetadata(String runId, String pipelineName, int batchSequence,
                                          String batchKey, String batchStrategy,
                                          int totalRecords, int validRecords, int malformedRecords,
                                          String startedAt, String completedAt) {
        String sql = "INSERT INTO batch_metadata (run_id, pipeline_name, batch_sequence, batch_key, " +
                     "batch_strategy, total_records, valid_records, malformed_records, started_at, completed_at) " +
                     "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (Connection conn = getConnection(true); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, runId);
            ps.setString(2, pipelineName);
            ps.setInt(3, batchSequence);
            ps.setString(4, batchKey);
            ps.setString(5, batchStrategy);
            ps.setInt(6, totalRecords);
            ps.setInt(7, validRecords);
            ps.setInt(8, malformedRecords);
            ps.setString(9, startedAt);
            ps.setString(10, completedAt);
            ps.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void printReport(String runId) {
        try (Connection conn = getConnection(true)) {
            System.out.println("\n================================================================================");
            System.out.println("  📊  RUN METADATA");
            System.out.println("================================================================================");
            String metaSql = "SELECT * FROM run_metadata WHERE run_id = ?";
            try (PreparedStatement ps = conn.prepareStatement(metaSql)) {
                ps.setString(1, runId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        System.out.println("  Run ID     : " + rs.getString("run_id"));
                        System.out.println("  Pipeline   : " + rs.getString("pipeline_name"));
                        System.out.println("  Batch Strat: " + rs.getString("batch_strategy"));
                        System.out.println("  Batch Size : " + rs.getInt("batch_size"));
                        System.out.println("  Total Recs : " + rs.getInt("total_records"));
                        System.out.println("  Malformed  : " + rs.getInt("malformed_records"));
                        System.out.println("  # Batches  : " + rs.getInt("num_batches"));
                        System.out.println("  Avg Batch  : " + rs.getFloat("avg_batch_size"));
                        System.out.println("  Runtime    : " + rs.getFloat("runtime_seconds") + "s");
                        System.out.println("  Started    : " + rs.getString("started_at"));
                        System.out.println("  Completed  : " + rs.getString("completed_at"));
                    }
                }
            }

            // Print batch metadata
            System.out.println("\n────────────────────────────────────────────────────────────────────────────────");
            System.out.println("  📦  BATCH METADATA");
            System.out.println("────────────────────────────────────────────────────────────────────────────────");
            System.out.printf("  │ %-6s │ %-12s │ %-10s │ %-8s │ %-8s │ %-10s │%n",
                    "Seq#", "Batch Key", "Strategy", "Total", "Valid", "Malformed");
            System.out.println("  │────────│──────────────│────────────│──────────│──────────│────────────│");
            String batchSql = "SELECT * FROM batch_metadata WHERE run_id = ? ORDER BY batch_sequence";
            try (PreparedStatement ps = conn.prepareStatement(batchSql)) {
                ps.setString(1, runId);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        System.out.printf("  │ %-6d │ %-12s │ %-10s │ %-8d │ %-8d │ %-10d │%n",
                                rs.getInt("batch_sequence"),
                                rs.getString("batch_key"),
                                rs.getString("batch_strategy"),
                                rs.getInt("total_records"),
                                rs.getInt("valid_records"),
                                rs.getInt("malformed_records"));
                    }
                }
            }

            // Print Q1
            System.out.println("\n────────────────────────────────────────────────────────────────────────────────");
            System.out.println("  📋  QUERY 1 — Daily Traffic Summary");
            System.out.println("────────────────────────────────────────────────────────────────────────────────");
            String q1Sql = "SELECT log_date, status_code, request_count, total_bytes FROM query1_results WHERE run_id = ? LIMIT 20";
            try (PreparedStatement ps = conn.prepareStatement(q1Sql)) {
                ps.setString(1, runId);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        System.out.printf("  │ %-12s │ %-10d │ %-13d │ %-13d │%n",
                                rs.getString("log_date"), rs.getInt("status_code"),
                                rs.getInt("request_count"), rs.getLong("total_bytes"));
                    }
                }
            }

            // Print Q2
            System.out.println("\n────────────────────────────────────────────────────────────────────────────────");
            System.out.println("  📋  QUERY 2 — Top 20 Requested Resources");
            System.out.println("────────────────────────────────────────────────────────────────────────────────");
            String q2Sql = "SELECT resource_path, request_count, total_bytes, distinct_host_count FROM query2_results WHERE run_id = ? ORDER BY request_count DESC LIMIT 20";
            try (PreparedStatement ps = conn.prepareStatement(q2Sql)) {
                ps.setString(1, runId);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String path = rs.getString("resource_path");
                        if (path.length() > 47) path = path.substring(0, 44) + "...";
                        System.out.printf("  │ %-47s │ %-13d │ %-13d │ %-19d │%n",
                                path, rs.getInt("request_count"),
                                rs.getLong("total_bytes"), rs.getInt("distinct_host_count"));
                    }
                }
            }

            // Print Q3
            System.out.println("\n────────────────────────────────────────────────────────────────────────────────");
            System.out.println("  📋  QUERY 3 — Hourly Error Analysis");
            System.out.println("────────────────────────────────────────────────────────────────────────────────");
            String q3Sql = "SELECT log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts FROM query3_results WHERE run_id = ? LIMIT 20";
            try (PreparedStatement ps = conn.prepareStatement(q3Sql)) {
                ps.setString(1, runId);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        System.out.printf("  │ %-12s │ %-8d │ %-11d │ %-11d │ %-10.4f │ %-14d │%n",
                                rs.getString("log_date"), rs.getInt("log_hour"),
                                rs.getInt("error_request_count"), rs.getInt("total_request_count"),
                                rs.getFloat("error_rate"), rs.getInt("distinct_error_hosts"));
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // ── Correctness Check ───────────────────────────────────────────────────
    /**
     * Compares query results between two run IDs (typically from different pipelines).
     * Checks all 3 query tables and reports whether the outputs match.
     */
    public static void runCorrectnessCheck(String runId1, String runId2) {
        System.out.println("\n================================================================================");
        System.out.println("  🔍  CORRECTNESS CHECK");
        System.out.println("================================================================================");
        System.out.println("  Comparing Run A: " + runId1);
        System.out.println("  Against  Run B: " + runId2);
        System.out.println();

        try (Connection conn = getConnection(true)) {
            // ── Metadata summary ────────────────────────────────────────
            String metaSql = "SELECT pipeline_name, total_records, malformed_records FROM run_metadata WHERE run_id = ?";
            for (String rid : new String[]{runId1, runId2}) {
                try (PreparedStatement ps = conn.prepareStatement(metaSql)) {
                    ps.setString(1, rid);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            System.out.printf("  %s [%s]: %d total, %d malformed%n",
                                    rid, rs.getString("pipeline_name"),
                                    rs.getInt("total_records"), rs.getInt("malformed_records"));
                        }
                    }
                }
            }

            boolean allMatch = true;

            // ── Q1: Compare aggregated totals by (log_date, status_code) ──
            System.out.println("\n  ─── Query 1 (Daily Traffic) ───");
            allMatch &= compareQuery(conn,
                "SELECT log_date, status_code, SUM(request_count) rc, SUM(total_bytes) tb " +
                "FROM query1_results WHERE run_id = ? GROUP BY log_date, status_code ORDER BY log_date, status_code",
                runId1, runId2, "Q1");

            // ── Q2: Compare top-20 resources by request_count ──
            System.out.println("  ─── Query 2 (Top Resources) ───");
            allMatch &= compareQuery(conn,
                "SELECT resource_path, request_count, total_bytes, distinct_host_count " +
                "FROM query2_results WHERE run_id = ? ORDER BY request_count DESC, resource_path ASC LIMIT 20",
                runId1, runId2, "Q2");

            // ── Q3: Compare hourly error analysis ──
            System.out.println("  ─── Query 3 (Hourly Errors) ───");
            allMatch &= compareQuery(conn,
                "SELECT log_date, log_hour, SUM(error_request_count) ec, SUM(total_request_count) tc " +
                "FROM query3_results WHERE run_id = ? GROUP BY log_date, log_hour ORDER BY log_date, log_hour",
                runId1, runId2, "Q3");

            System.out.println();
            if (allMatch) {
                System.out.println("  ✅  ALL QUERIES MATCH — Both pipelines produce identical output.");
            } else {
                System.out.println("  ❌  MISMATCH DETECTED — See details above.");
            }
            System.out.println("================================================================================\n");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static boolean compareQuery(Connection conn, String sql, String rid1, String rid2, String label) {
        try {
            // Fetch rows for run 1
            List<String> rows1 = new java.util.ArrayList<>();
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, rid1);
                try (ResultSet rs = ps.executeQuery()) {
                    int colCount = rs.getMetaData().getColumnCount();
                    while (rs.next()) {
                        StringBuilder sb = new StringBuilder();
                        for (int c = 1; c <= colCount; c++) {
                            if (c > 1) sb.append("|");
                            sb.append(rs.getString(c));
                        }
                        rows1.add(sb.toString());
                    }
                }
            }

            // Fetch rows for run 2
            List<String> rows2 = new java.util.ArrayList<>();
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, rid2);
                try (ResultSet rs = ps.executeQuery()) {
                    int colCount = rs.getMetaData().getColumnCount();
                    while (rs.next()) {
                        StringBuilder sb = new StringBuilder();
                        for (int c = 1; c <= colCount; c++) {
                            if (c > 1) sb.append("|");
                            sb.append(rs.getString(c));
                        }
                        rows2.add(sb.toString());
                    }
                }
            }

            System.out.printf("      Run A rows: %d, Run B rows: %d%n", rows1.size(), rows2.size());
            if (rows1.size() != rows2.size()) {
                System.out.println("      ❌ " + label + " — Row count mismatch!");
                // Show first few diffs
                int max = Math.min(5, Math.max(rows1.size(), rows2.size()));
                for (int i = 0; i < max; i++) {
                    String a = i < rows1.size() ? rows1.get(i) : "(missing)";
                    String b = i < rows2.size() ? rows2.get(i) : "(missing)";
                    if (!a.equals(b)) {
                        System.out.println("        Row " + (i+1) + " A: " + a);
                        System.out.println("        Row " + (i+1) + " B: " + b);
                    }
                }
                return false;
            }

            int mismatches = 0;
            for (int i = 0; i < rows1.size(); i++) {
                if (!rows1.get(i).equals(rows2.get(i))) {
                    mismatches++;
                    if (mismatches <= 5) {
                        System.out.println("        Diff row " + (i+1) + ":");
                        System.out.println("          A: " + rows1.get(i));
                        System.out.println("          B: " + rows2.get(i));
                    }
                }
            }

            if (mismatches == 0) {
                System.out.println("      ✅ " + label + " — MATCH (" + rows1.size() + " rows)");
                return true;
            } else {
                System.out.println("      ❌ " + label + " — " + mismatches + " row(s) differ");
                return false;
            }

        } catch (Exception e) {
            System.out.println("      ❌ " + label + " — Error during comparison: " + e.getMessage());
            return false;
        }
    }
}
