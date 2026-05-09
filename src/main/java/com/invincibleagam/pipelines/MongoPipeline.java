package com.invincibleagam.pipelines;

import com.invincibleagam.core.BatchStrategy;
import com.invincibleagam.core.Config;
import com.invincibleagam.core.DatabaseManager;
import com.invincibleagam.core.LogParser;
import com.invincibleagam.core.LogicalBatchProcessor;
import com.invincibleagam.models.ParsedLog;
import com.mongodb.client.*;
import com.mongodb.client.model.*;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class MongoPipeline {

    /**
     * @param filepaths       log file paths
     * @param batchSize       fixed batch size (used only when strategy == FIXED)
     * @param strategy        batching strategy (FIXED, MONTHLY, WEEKLY)
     * @param selectedQueries set of queries to run, e.g. {"q1","q2","q3"}
     */
    public static String runPipeline(List<String> filepaths, int batchSize,
                                      BatchStrategy strategy, Set<String> selectedQueries) {
        String runId = "mongo_" + UUID.randomUUID().toString().substring(0, 8);
        String pipelineName = "mongodb";
        System.out.println("\n======================================================================");
        System.out.println("  🍃  MongoDB Pipeline (Java) — Run ID: " + runId);
        System.out.println("      Batch Strategy: " + strategy.getLabel());
        System.out.println("      Queries: " + selectedQueries);
        System.out.println("======================================================================");

        DatabaseManager.createDatabaseAndTables();

        String mongoUrl = "mongodb://" + Config.MONGO_HOST + ":" + Config.MONGO_PORT;
        try (MongoClient mongoClient = MongoClients.create(mongoUrl)) {
            MongoDatabase database = mongoClient.getDatabase(Config.MONGO_DATABASE);
            MongoCollection<Document> collection = database.getCollection(Config.MONGO_COLLECTION);

            System.out.println("  ✓  MongoDB collection reset");
            collection.drop(); // Reset for new run

            long startMillis = System.currentTimeMillis();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            String startedAt = LocalDateTime.now().format(formatter);

            System.out.println("  ⏳ Parsing log files & loading to MongoDB...");
            LogicalBatchProcessor processor = new LogicalBatchProcessor(filepaths, strategy, batchSize);

            int totalRecords = 0;
            int malformedCount = 0;
            int numBatches = 0;
            int lastBatchId = 0;

            for (Map.Entry<String, List<String>> entry : processor) {
                numBatches++;
                lastBatchId = numBatches;
                String batchKey = entry.getKey();
                List<String> batchLines = entry.getValue();
                totalRecords += batchLines.size();

                String batchStartTime = LocalDateTime.now().format(formatter);
                int batchMalformed = 0;

                List<Document> docsToInsert = new ArrayList<>();
                for (String line : batchLines) {
                    ParsedLog parsed = LogParser.parse(line);
                    if (parsed == null) {
                        malformedCount++;
                        batchMalformed++;
                    } else {
                        Document doc = new Document("host", parsed.host)
                                .append("timestamp", parsed.timestamp)
                                .append("log_date", parsed.logDate)
                                .append("log_hour", parsed.logHour)
                                .append("http_method", parsed.httpMethod)
                                .append("resource_path", parsed.resourcePath)
                                .append("protocol_version", parsed.protocolVersion)
                                .append("status_code", parsed.statusCode)
                                .append("bytes_transferred", parsed.bytesTransferred);
                        docsToInsert.add(doc);
                    }
                }

                if (!docsToInsert.isEmpty()) {
                    collection.insertMany(docsToInsert);
                }

                String batchEndTime = LocalDateTime.now().format(formatter);

                // Record batch metadata
                DatabaseManager.loadBatchMetadata(runId, pipelineName, numBatches, batchKey,
                        strategy.getLabel(), batchLines.size(), docsToInsert.size(),
                        batchMalformed, batchStartTime, batchEndTime);

                if (numBatches % 50 == 0 || strategy != BatchStrategy.FIXED) {
                    System.out.printf("      Batch %d [%s]: %d lines (%d malformed)%n",
                            numBatches, batchKey, batchLines.size(), batchMalformed);
                }
            }

            System.out.printf("  ✓  Inserted %d docs (%d malformed, %d batches)%n",
                    (totalRecords - malformedCount), malformedCount, numBatches);

            String execTime = LocalDateTime.now().format(formatter);

            try (Connection conn = DriverManager.getConnection("jdbc:mysql://" + Config.MYSQL_HOST + ":" + Config.MYSQL_PORT + "/" + Config.MYSQL_DATABASE, Config.MYSQL_USER, Config.MYSQL_PASSWORD)) {

                // ─── Query 1 ──────────────────────────────────────────────────
                if (selectedQueries.contains("q1")) {
                    System.out.println("  ⏳ Running Query 1 (MongoDB Aggregation)...");
                    List<Bson> pipeline1 = Arrays.asList(
                            Aggregates.group(
                                    new Document("log_date", "$log_date").append("status_code", "$status_code"),
                                    Accumulators.sum("request_count", 1),
                                    Accumulators.sum("total_bytes", "$bytes_transferred")
                            ),
                            Aggregates.sort(Sorts.ascending("_id.log_date", "_id.status_code"))
                    );

                    String sql1 = "INSERT INTO query1_results (pipeline_name, run_id, batch_id, execution_time, log_date, status_code, request_count, total_bytes) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
                    try (PreparedStatement ps = conn.prepareStatement(sql1)) {
                        int rows = 0;
                        for (Document doc : collection.aggregate(pipeline1)) {
                            Document id = (Document) doc.get("_id");
                            ps.setString(1, pipelineName);
                            ps.setString(2, runId);
                            ps.setInt(3, lastBatchId);
                            ps.setString(4, execTime);
                            ps.setString(5, id.getString("log_date"));
                            ps.setInt(6, id.getInteger("status_code"));
                            ps.setInt(7, doc.getInteger("request_count"));
                            ps.setLong(8, doc.getLong("total_bytes"));
                            ps.addBatch();
                            rows++;
                        }
                        ps.executeBatch();
                        System.out.println("      → " + rows + " rows loaded to MySQL");
                    }
                } else {
                    System.out.println("  ⏭  Skipping Query 1");
                }

                // ─── Query 2 ──────────────────────────────────────────────────
                if (selectedQueries.contains("q2")) {
                    System.out.println("  ⏳ Running Query 2 (MongoDB Aggregation)...");
                    List<Bson> pipeline2 = Arrays.asList(
                            Aggregates.group(
                                    "$resource_path",
                                    Accumulators.sum("request_count", 1),
                                    Accumulators.sum("total_bytes", "$bytes_transferred"),
                                    Accumulators.addToSet("distinct_hosts", "$host")
                            ),
                            Aggregates.project(new Document("resource_path", "$_id")
                                    .append("request_count", 1)
                                    .append("total_bytes", 1)
                                    .append("distinct_host_count", new Document("$size", "$distinct_hosts"))
                            ),
                            Aggregates.sort(Sorts.descending("request_count")),
                            Aggregates.limit(20)
                    );

                    String sql2 = "INSERT INTO query2_results (pipeline_name, run_id, batch_id, execution_time, resource_path, request_count, total_bytes, distinct_host_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
                    try (PreparedStatement ps = conn.prepareStatement(sql2)) {
                        int rows = 0;
                        for (Document doc : collection.aggregate(pipeline2)) {
                            ps.setString(1, pipelineName);
                            ps.setString(2, runId);
                            ps.setInt(3, lastBatchId);
                            ps.setString(4, execTime);
                            String resource = doc.getString("resource_path");
                            ps.setString(5, resource.length() > 512 ? resource.substring(0, 512) : resource);
                            ps.setInt(6, doc.getInteger("request_count"));
                            ps.setLong(7, doc.getLong("total_bytes"));
                            ps.setInt(8, doc.getInteger("distinct_host_count"));
                            ps.addBatch();
                            rows++;
                        }
                        ps.executeBatch();
                        System.out.println("      → " + rows + " rows loaded to MySQL");
                    }
                } else {
                    System.out.println("  ⏭  Skipping Query 2");
                }

                // ─── Query 3 ──────────────────────────────────────────────────
                if (selectedQueries.contains("q3")) {
                    System.out.println("  ⏳ Running Query 3 (MongoDB Aggregation)...");
                    List<Bson> pipeline3 = Arrays.asList(
                            Aggregates.group(
                                    new Document("log_date", "$log_date").append("log_hour", "$log_hour"),
                                    Accumulators.sum("total_request_count", 1),
                                    Accumulators.sum("error_request_count",
                                            new Document("$cond", Arrays.asList(
                                                    new Document("$and", Arrays.asList(
                                                            new Document("$gte", Arrays.asList("$status_code", 400)),
                                                            new Document("$lte", Arrays.asList("$status_code", 599))
                                                    )), 1, 0
                                            ))
                                    ),
                                    Accumulators.addToSet("error_hosts",
                                            new Document("$cond", Arrays.asList(
                                                    new Document("$and", Arrays.asList(
                                                            new Document("$gte", Arrays.asList("$status_code", 400)),
                                                            new Document("$lte", Arrays.asList("$status_code", 599))
                                                    )), "$host", "$$REMOVE"
                                            ))
                                    )
                            ),
                            Aggregates.project(new Document("log_date", "$_id.log_date")
                                    .append("log_hour", "$_id.log_hour")
                                    .append("total_request_count", 1)
                                    .append("error_request_count", 1)
                                    .append("error_rate", new Document("$cond", Arrays.asList(
                                            new Document("$gt", Arrays.asList("$total_request_count", 0)),
                                            new Document("$divide", Arrays.asList("$error_request_count", "$total_request_count")),
                                            0
                                    )))
                                    .append("distinct_error_hosts", new Document("$size", new Document("$ifNull", Arrays.asList("$error_hosts", Collections.emptyList()))))
                            ),
                            Aggregates.sort(Sorts.ascending("log_date", "log_hour"))
                    );

                    String sql3 = "INSERT INTO query3_results (pipeline_name, run_id, batch_id, execution_time, log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                    try (PreparedStatement ps = conn.prepareStatement(sql3)) {
                        int rows = 0;
                        for (Document doc : collection.aggregate(pipeline3)) {
                            ps.setString(1, pipelineName);
                            ps.setString(2, runId);
                            ps.setInt(3, lastBatchId);
                            ps.setString(4, execTime);
                            ps.setString(5, doc.getString("log_date"));
                            ps.setInt(6, doc.getInteger("log_hour"));
                            ps.setInt(7, doc.getInteger("error_request_count"));
                            ps.setInt(8, doc.getInteger("total_request_count"));
                            ps.setDouble(9, doc.getDouble("error_rate"));
                            ps.setInt(10, doc.getInteger("distinct_error_hosts"));
                            ps.addBatch();
                            rows++;
                        }
                        ps.executeBatch();
                        System.out.println("      → " + rows + " rows loaded to MySQL");
                    }
                } else {
                    System.out.println("  ⏭  Skipping Query 3");
                }
            }

            long endMillis = System.currentTimeMillis();
            String completedAt = LocalDateTime.now().format(formatter);
            float runtimeSeconds = (endMillis - startMillis) / 1000f;
            float avgBatchSize = numBatches > 0 ? (float) totalRecords / numBatches : 0;

            DatabaseManager.loadRunMetadata(runId, pipelineName, strategy.getLabel(),
                    batchSize, totalRecords, malformedCount, numBatches, avgBatchSize,
                    runtimeSeconds, startedAt, completedAt);

            System.out.println("\n  ✅  MongoDB Pipeline Complete");
            System.out.println("      Runtime: " + runtimeSeconds + "s");
            System.out.println("      Total Malformed Records: " + malformedCount);

            return runId;

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
