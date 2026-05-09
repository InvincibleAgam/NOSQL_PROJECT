package com.invincibleagam.pipelines;

import com.invincibleagam.core.*;
import com.invincibleagam.models.ParsedLog;
import java.io.*;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class PigPipeline {

    public static String runPipeline(List<String> filepaths, int batchSize,
                                      BatchStrategy strategy, Set<String> selectedQueries) {
        String runId = "pig_" + UUID.randomUUID().toString().substring(0, 8);
        String pipelineName = "pig";
        System.out.println("\n======================================================================");
        System.out.println("  🐷  Apache Pig Pipeline (Java) — Run ID: " + runId);
        System.out.println("      Batch Strategy: " + strategy.getLabel());
        System.out.println("      Queries: " + selectedQueries);
        System.out.println("======================================================================");

        DatabaseManager.createDatabaseAndTables();
        long startMillis = System.currentTimeMillis();
        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String startedAt = LocalDateTime.now().format(fmt);

        // Stage input
        System.out.println("  ⏳ Parsing log files & staging for Pig...");
        File inputDir = new File("data/pig_input");
        if (inputDir.exists()) deleteDirectory(inputDir);
        inputDir.mkdirs();

        int totalRecords = 0, malformedCount = 0, numBatches = 0, lastBatchId = 0;
        LogicalBatchProcessor processor = new LogicalBatchProcessor(filepaths, strategy, batchSize);

        try (BufferedWriter w = new BufferedWriter(new FileWriter(new File(inputDir, "pig_input.tsv")))) {
            for (Map.Entry<String, List<String>> entry : processor) {
                numBatches++; lastBatchId = numBatches;
                String batchKey = entry.getKey();
                List<String> batchLines = entry.getValue();
                totalRecords += batchLines.size();
                String bStart = LocalDateTime.now().format(fmt);
                int bMal = 0, bVal = 0;
                for (String line : batchLines) {
                    ParsedLog p = LogParser.parse(line);
                    if (p == null) { malformedCount++; bMal++; }
                    else {
                        bVal++;
                        w.write(p.host.replace("\t"," ") + "\t" + p.timestamp + "\t" + p.logDate + "\t" +
                                p.logHour + "\t" + p.httpMethod + "\t" + p.resourcePath.replace("\t"," ") + "\t" +
                                p.protocolVersion + "\t" + p.statusCode + "\t" + p.bytesTransferred + "\n");
                    }
                }
                String bEnd = LocalDateTime.now().format(fmt);
                DatabaseManager.loadBatchMetadata(runId, pipelineName, numBatches, batchKey,
                        strategy.getLabel(), batchLines.size(), bVal, bMal, bStart, bEnd);
                if (numBatches % 50 == 0 || strategy != BatchStrategy.FIXED)
                    System.out.printf("      Batch %d [%s]: %d lines (%d malformed)%n", numBatches, batchKey, batchLines.size(), bMal);
            }
        } catch (Exception e) { e.printStackTrace(); return null; }

        System.out.printf("  ✓  Staged %d records (%d malformed, %d batches)%n", totalRecords - malformedCount, malformedCount, numBatches);
        String execTime = LocalDateTime.now().format(fmt);
        String absInput = new File(inputDir, "pig_input.tsv").getAbsolutePath();

        try (Connection conn = DriverManager.getConnection(
                "jdbc:mysql://" + Config.MYSQL_HOST + ":" + Config.MYSQL_PORT + "/" + Config.MYSQL_DATABASE,
                Config.MYSQL_USER, Config.MYSQL_PASSWORD)) {

            if (selectedQueries.contains("q1")) {
                System.out.println("  ⏳ Running Query 1 (Apache Pig)...");
                String out = new File("data/pig_output_q1").getAbsolutePath();
                deleteDirectory(new File(out));
                runPigScript("scripts/pig/pig_q1.pig", absInput, out);
                loadQ1(conn, pipelineName, runId, lastBatchId, execTime, out);
                deleteDirectory(new File(out));
            } else System.out.println("  ⏭  Skipping Query 1");

            if (selectedQueries.contains("q2")) {
                System.out.println("  ⏳ Running Query 2 (Apache Pig)...");
                String out = new File("data/pig_output_q2").getAbsolutePath();
                deleteDirectory(new File(out));
                runPigScript("scripts/pig/pig_q2.pig", absInput, out);
                loadQ2(conn, pipelineName, runId, lastBatchId, execTime, out);
                deleteDirectory(new File(out));
            } else System.out.println("  ⏭  Skipping Query 2");

            if (selectedQueries.contains("q3")) {
                System.out.println("  ⏳ Running Query 3 (Apache Pig)...");
                String out = new File("data/pig_output_q3").getAbsolutePath();
                deleteDirectory(new File(out));
                runPigScript("scripts/pig/pig_q3.pig", absInput, out);
                loadQ3(conn, pipelineName, runId, lastBatchId, execTime, out);
                deleteDirectory(new File(out));
            } else System.out.println("  ⏭  Skipping Query 3");

        } catch (Exception e) { e.printStackTrace(); return null; }

        deleteDirectory(inputDir);
        long endMillis = System.currentTimeMillis();
        String completedAt = LocalDateTime.now().format(fmt);
        float runtime = (endMillis - startMillis) / 1000f;
        float avgBatch = numBatches > 0 ? (float) totalRecords / numBatches : 0;

        DatabaseManager.loadRunMetadata(runId, pipelineName, strategy.getLabel(),
                batchSize, totalRecords, malformedCount, numBatches, avgBatch, runtime, startedAt, completedAt);

        System.out.println("\n  ✅  Apache Pig Pipeline Complete");
        System.out.printf("      Runtime: %.1fs | Malformed: %d%n", runtime, malformedCount);
        return runId;
    }

    private static void runPigScript(String script, String input, String output) throws Exception {
        ProcessBuilder pb = new ProcessBuilder("pig", "-x", "local", "-f",
                new File(script).getAbsolutePath(), "-param", "INPUT=" + input, "-param", "OUTPUT=" + output);
        pb.inheritIO();
        int exit = pb.start().waitFor();
        if (exit != 0) throw new Exception("Pig failed (exit " + exit + "): " + script);
    }

    private static List<String> readOutput(String dir) throws Exception {
        List<String> lines = new ArrayList<>();
        File d = new File(dir);
        if (!d.exists()) return lines;
        File[] parts = d.listFiles((x, n) -> n.startsWith("part-"));
        if (parts == null) return lines;
        Arrays.sort(parts);
        for (File f : parts) {
            try (BufferedReader r = new BufferedReader(new FileReader(f))) {
                String l; while ((l = r.readLine()) != null) if (!l.trim().isEmpty()) lines.add(l);
            }
        }
        return lines;
    }

    private static void loadQ1(Connection c, String pipe, String rid, int bid, String et, String dir) throws Exception {
        String sql = "INSERT INTO query1_results (pipeline_name,run_id,batch_id,execution_time,log_date,status_code,request_count,total_bytes) VALUES (?,?,?,?,?,?,?,?)";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            int rows = 0;
            for (String res : readOutput(dir)) {
                String[] p = res.split("\t"); if (p.length < 4) continue;
                ps.setString(1,pipe); ps.setString(2,rid); ps.setInt(3,bid); ps.setString(4,et);
                ps.setString(5,p[0]); ps.setInt(6,Integer.parseInt(p[1]));
                ps.setInt(7,Integer.parseInt(p[2])); ps.setLong(8,Long.parseLong(p[3]));
                ps.addBatch(); rows++;
            }
            ps.executeBatch();
            System.out.println("      → " + rows + " rows loaded to MySQL");
        }
    }

    private static void loadQ2(Connection c, String pipe, String rid, int bid, String et, String dir) throws Exception {
        String sql = "INSERT INTO query2_results (pipeline_name,run_id,batch_id,execution_time,resource_path,request_count,total_bytes,distinct_host_count) VALUES (?,?,?,?,?,?,?,?)";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            int rows = 0;
            for (String res : readOutput(dir)) {
                String[] p = res.split("\t"); if (p.length < 4) continue;
                ps.setString(1,pipe); ps.setString(2,rid); ps.setInt(3,bid); ps.setString(4,et);
                String rp = p[0]; ps.setString(5, rp.length() > 512 ? rp.substring(0,512) : rp);
                ps.setInt(6,Integer.parseInt(p[1])); ps.setLong(7,Long.parseLong(p[2]));
                ps.setInt(8,Integer.parseInt(p[3]));
                ps.addBatch(); rows++;
            }
            ps.executeBatch();
            System.out.println("      → " + rows + " rows loaded to MySQL");
        }
    }

    private static void loadQ3(Connection c, String pipe, String rid, int bid, String et, String dir) throws Exception {
        String sql = "INSERT INTO query3_results (pipeline_name,run_id,batch_id,execution_time,log_date,log_hour,error_request_count,total_request_count,error_rate,distinct_error_hosts) VALUES (?,?,?,?,?,?,?,?,?,?)";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            int rows = 0;
            for (String res : readOutput(dir)) {
                String[] p = res.split("\t"); if (p.length < 6) continue;
                ps.setString(1,pipe); ps.setString(2,rid); ps.setInt(3,bid); ps.setString(4,et);
                ps.setString(5,p[0]); ps.setInt(6,Integer.parseInt(p[1]));
                ps.setInt(7,Integer.parseInt(p[2])); ps.setInt(8,Integer.parseInt(p[3]));
                ps.setDouble(9,Double.parseDouble(p[4])); ps.setInt(10,Integer.parseInt(p[5]));
                ps.addBatch(); rows++;
            }
            ps.executeBatch();
            System.out.println("      → " + rows + " rows loaded to MySQL");
        }
    }

    private static void deleteDirectory(File dir) {
        if (dir.isDirectory()) { File[] fs = dir.listFiles(); if (fs != null) for (File f : fs) deleteDirectory(f); }
        dir.delete();
    }
}
