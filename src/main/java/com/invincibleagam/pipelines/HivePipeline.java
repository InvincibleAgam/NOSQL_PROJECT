package com.invincibleagam.pipelines;

import com.invincibleagam.core.*;
import com.invincibleagam.models.ParsedLog;
import java.io.*;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class HivePipeline {

    public static String runPipeline(List<String> filepaths, int batchSize,
                                      BatchStrategy strategy, Set<String> selectedQueries) {
        String runId = "hive_" + UUID.randomUUID().toString().substring(0, 8);
        String pipelineName = "hive";
        System.out.println("\n======================================================================");
        System.out.println("  🐝  Apache Hive Pipeline (Java) — Run ID: " + runId);
        System.out.println("      Batch Strategy: " + strategy.getLabel());
        System.out.println("      Queries: " + selectedQueries);
        System.out.println("======================================================================");

        DatabaseManager.createDatabaseAndTables();
        long startMillis = System.currentTimeMillis();
        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String startedAt = LocalDateTime.now().format(fmt);

        // Stage input
        System.out.println("  ⏳ Parsing log files & staging for Hive...");
        File inputDir = new File("data/hive_input");
        if (inputDir.exists()) deleteDirectory(inputDir);
        inputDir.mkdirs();

        int totalRecords = 0, malformedCount = 0, numBatches = 0, lastBatchId = 0;
        LogicalBatchProcessor processor = new LogicalBatchProcessor(filepaths, strategy, batchSize);

        try (BufferedWriter w = new BufferedWriter(new FileWriter(new File(inputDir, "hive_input.tsv")))) {
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
        String absInputDir = inputDir.getAbsolutePath();

        // Execute Hive setup (create external table)
        try {
            System.out.println("  ⏳ Creating Hive external table...");
            runHiveScript("scripts/hive/hive_setup.hql", absInputDir, null);
            System.out.println("  ✓  Hive table nasa_logs created");
        } catch (Exception e) { e.printStackTrace(); return null; }

        try (Connection conn = DriverManager.getConnection(
                "jdbc:mysql://" + Config.MYSQL_HOST + ":" + Config.MYSQL_PORT + "/" + Config.MYSQL_DATABASE,
                Config.MYSQL_USER, Config.MYSQL_PASSWORD)) {

            if (selectedQueries.contains("q1")) {
                System.out.println("  ⏳ Running Query 1 (Apache Hive)...");
                String out = new File("data/hive_output_q1").getAbsolutePath();
                deleteDirectory(new File(out));
                runHiveScript("scripts/hive/hive_q1.hql", absInputDir, out);
                loadQ1(conn, pipelineName, runId, lastBatchId, execTime, out);
                deleteDirectory(new File(out));
            } else System.out.println("  ⏭  Skipping Query 1");

            if (selectedQueries.contains("q2")) {
                System.out.println("  ⏳ Running Query 2 (Apache Hive)...");
                String out = new File("data/hive_output_q2").getAbsolutePath();
                deleteDirectory(new File(out));
                runHiveScript("scripts/hive/hive_q2.hql", absInputDir, out);
                loadQ2(conn, pipelineName, runId, lastBatchId, execTime, out);
                deleteDirectory(new File(out));
            } else System.out.println("  ⏭  Skipping Query 2");

            if (selectedQueries.contains("q3")) {
                System.out.println("  ⏳ Running Query 3 (Apache Hive)...");
                String out = new File("data/hive_output_q3").getAbsolutePath();
                deleteDirectory(new File(out));
                runHiveScript("scripts/hive/hive_q3.hql", absInputDir, out);
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

        System.out.println("\n  ✅  Apache Hive Pipeline Complete");
        System.out.printf("      Runtime: %.1fs | Malformed: %d%n", runtime, malformedCount);
        return runId;
    }

    private static void runHiveScript(String script, String inputDir, String outputDir) throws Exception {
        List<String> cmd = new ArrayList<>();
        cmd.add("beeline");
        cmd.add("-u"); cmd.add("jdbc:hive2://");
        cmd.add("--hivevar"); cmd.add("INPUT_DIR=" + inputDir);
        if (outputDir != null) { cmd.add("--hivevar"); cmd.add("OUTPUT_DIR=" + outputDir); }
        cmd.add("-f"); cmd.add(new File(script).getAbsolutePath());

        ProcessBuilder pb = new ProcessBuilder(cmd);
        // -Dorg.jline.terminal.provider=dumb works around Hive 4.2 beeline's JLine JNA crash on macOS arm64
        pb.environment().put("HADOOP_CLIENT_OPTS",
                "-Dhadoop.service.shutdown.timeout=120000 -Dorg.jline.terminal.provider=dumb");
        pb.environment().put("HIVE_CONF_DIR", System.getProperty("user.dir"));
        pb.inheritIO();
        int exit = pb.start().waitFor();
        if (exit != 0) throw new Exception("Hive failed (exit " + exit + "): " + script);
    }

    private static List<String> readOutput(String dir) throws Exception {
        List<String> lines = new ArrayList<>();
        File d = new File(dir);
        if (!d.exists()) return lines;
        File[] files = d.listFiles();
        if (files == null) return lines;
        Arrays.sort(files);
        for (File f : files) {
            if (f.isFile() && !f.getName().startsWith(".") && !f.getName().equals("_SUCCESS")) {
                try (BufferedReader r = new BufferedReader(new FileReader(f))) {
                    String l; while ((l = r.readLine()) != null) if (!l.trim().isEmpty()) lines.add(l);
                }
            }
        }
        return lines;
    }

    private static void loadQ1(Connection c, String pipe, String rid, int bid, String et, String dir) throws Exception {
        String sql = "INSERT INTO query1_results (pipeline_name,run_id,batch_id,execution_time,log_date,status_code,request_count,total_bytes) VALUES (?,?,?,?,?,?,?,?)";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            int rows = 0;
            for (String res : readOutput(dir)) {
                // Hive uses \001 (Ctrl-A) as default separator for INSERT OVERWRITE LOCAL DIRECTORY
                // but we specified ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
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
