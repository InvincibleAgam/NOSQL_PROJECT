package com.invincibleagam.pipelines;

import com.invincibleagam.core.BatchProcessor;
import com.invincibleagam.core.Config;
import com.invincibleagam.core.DatabaseManager;
import com.invincibleagam.core.LogParser;
import com.invincibleagam.models.ParsedLog;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

public class MapReducePipeline {

    public static String runPipeline(List<String> filepaths, int batchSize) {
        String runId = "mr_" + UUID.randomUUID().toString().substring(0, 8);
        String pipelineName = "mapreduce";
        System.out.println("\n======================================================================");
        System.out.println("  🗺️  MapReduce Pipeline (Java) — Run ID: " + runId);
        System.out.println("======================================================================");

        DatabaseManager.createDatabaseAndTables();

        long startMillis = System.currentTimeMillis();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String startedAt = LocalDateTime.now().format(formatter);

        // Stage Input for Hadoop
        System.out.println("  ⏳ Parsing log files & staging for Hadoop...");
        File inputDir = new File("data/mr_input");
        if (inputDir.exists()) {
            deleteDirectory(inputDir);
        }
        inputDir.mkdirs();

        int totalRecords = 0;
        int malformedCount = 0;
        int numBatches = 0;
        int lastBatchId = 0;

        BatchProcessor processor = new BatchProcessor(filepaths, batchSize);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(inputDir, "mr_input.tsv")))) {
            for (List<String> batchLines : processor) {
                numBatches++;
                lastBatchId = numBatches;
                totalRecords += batchLines.size();

                for (String line : batchLines) {
                    ParsedLog parsed = LogParser.parse(line);
                    if (parsed == null) {
                        malformedCount++;
                    } else {
                        String safeHost = parsed.host.replace("\t", " ");
                        String safeResource = parsed.resourcePath.replace("\t", " ");
                        writer.write(safeHost + "\t" + parsed.timestamp + "\t" + parsed.logDate + "\t" +
                                parsed.logHour + "\t" + parsed.httpMethod + "\t" + safeResource + "\t" +
                                parsed.protocolVersion + "\t" + parsed.statusCode + "\t" + parsed.bytesTransferred + "\n");
                    }
                }
                if (numBatches % 50 == 0) {
                    System.out.println("      Batch " + numBatches + ": " + batchLines.size() + " lines staged");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

        System.out.printf("  ✓  Staged %d records for Hadoop (%d malformed, %d batches)%n",
                (totalRecords - malformedCount), malformedCount, numBatches);

        String execTime = LocalDateTime.now().format(formatter);

        try (Connection conn = DriverManager.getConnection("jdbc:mysql://" + Config.MYSQL_HOST + ":" + Config.MYSQL_PORT + "/" + Config.MYSQL_DATABASE, Config.MYSQL_USER, Config.MYSQL_PASSWORD)) {
            
            // ─── Query 1 ──────────────────────────────────────────────────
            System.out.println("  ⏳ Running Query 1 (Hadoop MapReduce)...");
            String outQ1 = "data/mr_output_q1";
            runHadoopJob("q1", inputDir.getPath(), outQ1);

            String sql1 = "INSERT INTO query1_results (pipeline_name, run_id, batch_id, execution_time, log_date, status_code, request_count, total_bytes) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
            try (PreparedStatement ps = conn.prepareStatement(sql1)) {
                int rows = 0;
                List<String> results = readHadoopOutput(outQ1);
                for (String res : results) {
                    String[] parts = res.split("\t");
                    ps.setString(1, pipelineName);
                    ps.setString(2, runId);
                    ps.setInt(3, lastBatchId);
                    ps.setString(4, execTime);
                    ps.setString(5, parts[0]);
                    ps.setInt(6, Integer.parseInt(parts[1]));
                    ps.setInt(7, Integer.parseInt(parts[2]));
                    ps.setLong(8, Long.parseLong(parts[3]));
                    ps.addBatch();
                    rows++;
                }
                ps.executeBatch();
                System.out.println("      → " + rows + " rows loaded to MySQL");
            }

            // ─── Query 2 ──────────────────────────────────────────────────
            System.out.println("  ⏳ Running Query 2 (Hadoop MapReduce)...");
            String outQ2 = "data/mr_output_q2";
            runHadoopJob("q2", inputDir.getPath(), outQ2);

            String sql2 = "INSERT INTO query2_results (pipeline_name, run_id, batch_id, execution_time, resource_path, request_count, total_bytes, distinct_host_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
            try (PreparedStatement ps = conn.prepareStatement(sql2)) {
                int rows = 0;
                List<String> results = readHadoopOutput(outQ2);
                
                // Sort by request count DESC to get Top 20 (Simulating 2nd MapReduce Job)
                List<String[]> parsedResults = new ArrayList<>();
                for (String res : results) {
                    parsedResults.add(res.split("\t"));
                }
                parsedResults.sort((a, b) -> Integer.compare(Integer.parseInt(b[1]), Integer.parseInt(a[1])));

                for (int i = 0; i < Math.min(20, parsedResults.size()); i++) {
                    String[] parts = parsedResults.get(i);
                    ps.setString(1, pipelineName);
                    ps.setString(2, runId);
                    ps.setInt(3, lastBatchId);
                    ps.setString(4, execTime);
                    String resource = parts[0];
                    ps.setString(5, resource.length() > 512 ? resource.substring(0, 512) : resource);
                    ps.setInt(6, Integer.parseInt(parts[1]));
                    ps.setLong(7, Long.parseLong(parts[2]));
                    ps.setInt(8, Integer.parseInt(parts[3]));
                    ps.addBatch();
                    rows++;
                }
                ps.executeBatch();
                System.out.println("      → " + rows + " rows loaded to MySQL");
            }

            // ─── Query 3 ──────────────────────────────────────────────────
            System.out.println("  ⏳ Running Query 3 (Hadoop MapReduce)...");
            String outQ3 = "data/mr_output_q3";
            runHadoopJob("q3", inputDir.getPath(), outQ3);

            String sql3 = "INSERT INTO query3_results (pipeline_name, run_id, batch_id, execution_time, log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            try (PreparedStatement ps = conn.prepareStatement(sql3)) {
                int rows = 0;
                List<String> results = readHadoopOutput(outQ3);
                for (String res : results) {
                    String[] parts = res.split("\t");
                    ps.setString(1, pipelineName);
                    ps.setString(2, runId);
                    ps.setInt(3, lastBatchId);
                    ps.setString(4, execTime);
                    ps.setString(5, parts[0]);
                    ps.setInt(6, Integer.parseInt(parts[1]));
                    ps.setInt(7, Integer.parseInt(parts[2]));
                    ps.setInt(8, Integer.parseInt(parts[3]));
                    ps.setDouble(9, Double.parseDouble(parts[4]));
                    ps.setInt(10, Integer.parseInt(parts[5]));
                    ps.addBatch();
                    rows++;
                }
                ps.executeBatch();
                System.out.println("      → " + rows + " rows loaded to MySQL");
            }

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

        // Cleanup
        deleteDirectory(inputDir);
        deleteDirectory(new File("data/mr_output_q1"));
        deleteDirectory(new File("data/mr_output_q2"));
        deleteDirectory(new File("data/mr_output_q3"));

        long endMillis = System.currentTimeMillis();
        String completedAt = LocalDateTime.now().format(formatter);
        float runtimeSeconds = (endMillis - startMillis) / 1000f;
        float avgBatchSize = numBatches > 0 ? (float) totalRecords / numBatches : 0;

        DatabaseManager.loadRunMetadata(runId, pipelineName, batchSize, totalRecords, malformedCount, numBatches, avgBatchSize, runtimeSeconds, startedAt, completedAt);

        System.out.println("\n  ✅  Java MapReduce Pipeline Complete");
        System.out.println("      Runtime: " + runtimeSeconds + "s");

        return runId;
    }

    private static void runHadoopJob(String queryId, String inPath, String outPath) throws Exception {
        File outDir = new File(outPath);
        if (outDir.exists()) {
            deleteDirectory(outDir);
        }
        
        // Execute the job via hadoop jar
        ProcessBuilder pb = new ProcessBuilder(
            "hadoop", "jar", "target/nosql-project-1.0-SNAPSHOT-jar-with-dependencies.jar",
            "com.invincibleagam.pipelines.NASALogDriver", queryId, inPath, outPath
        );
        pb.inheritIO();
        Process p = pb.start();
        
        int exitCode = p.waitFor();
        if (exitCode != 0) {
            throw new Exception("Hadoop job failed with exit code " + exitCode);
        }
    }

    private static List<String> readHadoopOutput(String dir) throws Exception {
        List<String> lines = new ArrayList<>();
        File file = new File(dir, "part-r-00000");
        if (file.exists()) {
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (!line.trim().isEmpty()) {
                        lines.add(line);
                    }
                }
            }
        }
        return lines;
    }

    private static void deleteDirectory(File dir) {
        if (dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    deleteDirectory(file);
                }
            }
        }
        dir.delete();
    }
}
