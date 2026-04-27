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
                        "batch_size INT, " +
                        "total_records INT, " +
                        "malformed_records INT, " +
                        "num_batches INT, " +
                        "avg_batch_size FLOAT, " +
                        "runtime_seconds FLOAT, " +
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

    public static void loadRunMetadata(String runId, String pipelineName, int batchSize, int totalRecords,
                                       int malformedRecords, int numBatches, float avgBatchSize,
                                       float runtimeSeconds, String startedAt, String completedAt) {
        String sql = "INSERT INTO run_metadata (run_id, pipeline_name, batch_size, total_records, " +
                     "malformed_records, num_batches, avg_batch_size, runtime_seconds, started_at, completed_at) " +
                     "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (Connection conn = getConnection(true); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, runId);
            ps.setString(2, pipelineName);
            ps.setInt(3, batchSize);
            ps.setInt(4, totalRecords);
            ps.setInt(5, malformedRecords);
            ps.setInt(6, numBatches);
            ps.setFloat(7, avgBatchSize);
            ps.setFloat(8, runtimeSeconds);
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

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
