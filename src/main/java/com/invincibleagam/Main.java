package com.invincibleagam;

import com.invincibleagam.core.BatchStrategy;
import com.invincibleagam.core.Config;
import com.invincibleagam.core.DatabaseManager;
import com.invincibleagam.pipelines.HivePipeline;
import com.invincibleagam.pipelines.MapReducePipeline;
import com.invincibleagam.pipelines.MongoPipeline;
import com.invincibleagam.pipelines.PigPipeline;

import java.io.File;
import java.util.*;

public class Main {
    public static void main(String[] args) {
        System.out.println("================================================================================");
        System.out.println("  NASA HTTP Log Analysis Tool — NoSQL Systems Project (Java)");
        System.out.println("================================================================================\n");

        List<String> files = Arrays.asList(Config.JULY_LOG, Config.AUGUST_LOG);
        for (String f : files) {
            File file = new File(f);
            if (!file.exists()) {
                System.out.println("  ❌  Missing dataset file: " + f);
                System.out.println("      Please run scripts/download_data.sh first.");
                return;
            } else {
                System.out.println("  ✓  Found: " + f);
            }
        }

        Scanner scanner = new Scanner(System.in);
        System.out.println("\nSelect a pipeline:");
        System.out.println("  1. MongoDB Aggregation Pipeline");
        System.out.println("  2. Hadoop MapReduce Pipeline");
        System.out.println("  3. Apache Pig Pipeline");
        System.out.println("  4. Apache Hive Pipeline");
        System.out.println("  5. Run ALL 4 Pipelines + Correctness Check");
        System.out.println("  6. View Report for a Run");
        System.out.print("\nChoice [1-6]: ");

        String choice = scanner.nextLine().trim();

        if (choice.matches("[1-5]")) {

            // ── Batch Strategy ──────────────────────────────────────────
            System.out.println("\nBatch Strategy:");
            System.out.println("  1. Fixed size (default)");
            System.out.println("  2. Monthly (group by calendar month)");
            System.out.println("  3. Weekly  (group by ISO week)");
            System.out.print("Strategy [1-3]: ");
            String stratInput = scanner.nextLine().trim();
            BatchStrategy strategy;
            switch (stratInput) {
                case "2":  strategy = BatchStrategy.MONTHLY; break;
                case "3":  strategy = BatchStrategy.WEEKLY;  break;
                default:   strategy = BatchStrategy.FIXED;   break;
            }

            // ── Batch Size (only meaningful for FIXED) ──────────────────
            int batchSize = Config.DEFAULT_BATCH_SIZE;
            if (strategy == BatchStrategy.FIXED) {
                System.out.print("Batch size [" + Config.DEFAULT_BATCH_SIZE + "]: ");
                String batchInput = scanner.nextLine().trim();
                if (!batchInput.isEmpty()) {
                    try { batchSize = Integer.parseInt(batchInput); } catch (Exception ignored) {}
                }
            }

            // ── Query Selection ─────────────────────────────────────────
            System.out.println("\nSelect queries to run:");
            System.out.println("  a. All queries (q1, q2, q3)");
            System.out.println("  1. Query 1 only (Daily Traffic Summary)");
            System.out.println("  2. Query 2 only (Top Resources)");
            System.out.println("  3. Query 3 only (Hourly Error Analysis)");
            System.out.println("  c. Custom (comma-separated, e.g. 1,3)");
            System.out.print("Queries [a]: ");
            String queryInput = scanner.nextLine().trim().toLowerCase();
            Set<String> selectedQueries = new LinkedHashSet<>();

            if (queryInput.isEmpty() || queryInput.equals("a")) {
                selectedQueries.addAll(Arrays.asList("q1", "q2", "q3"));
            } else if (queryInput.equals("1")) {
                selectedQueries.add("q1");
            } else if (queryInput.equals("2")) {
                selectedQueries.add("q2");
            } else if (queryInput.equals("3")) {
                selectedQueries.add("q3");
            } else if (queryInput.startsWith("c")) {
                System.out.print("Enter query numbers (comma-separated, e.g. 1,3): ");
                String custom = scanner.nextLine().trim();
                for (String q : custom.split(",")) {
                    q = q.trim();
                    if (q.equals("1")) selectedQueries.add("q1");
                    if (q.equals("2")) selectedQueries.add("q2");
                    if (q.equals("3")) selectedQueries.add("q3");
                }
            }
            if (selectedQueries.isEmpty()) {
                selectedQueries.addAll(Arrays.asList("q1", "q2", "q3"));
            }

            // ── Execute ─────────────────────────────────────────────────
            if (choice.equals("5")) {
                // Run all 4 pipelines
                List<String> runIds = new ArrayList<>();
                String[] pipelineNames = {"MongoDB", "MapReduce", "Pig", "Hive"};
                int idx = 0;

                System.out.println("\n  ═══ Running all 4 pipelines sequentially ═══\n");

                String mongoId = MongoPipeline.runPipeline(files, batchSize, strategy, selectedQueries);
                if (mongoId != null) runIds.add(mongoId);

                String mrId = MapReducePipeline.runPipeline(files, batchSize, strategy, selectedQueries);
                if (mrId != null) runIds.add(mrId);

                String pigId = PigPipeline.runPipeline(files, batchSize, strategy, selectedQueries);
                if (pigId != null) runIds.add(pigId);

                String hiveId = HivePipeline.runPipeline(files, batchSize, strategy, selectedQueries);
                if (hiveId != null) runIds.add(hiveId);

                // Print reports
                System.out.println("\n  ═══ Reports ═══");
                for (String rid : runIds) {
                    DatabaseManager.printReport(rid);
                }

                // Pairwise correctness checks
                if (runIds.size() >= 2) {
                    System.out.println("\n  ═══ Pairwise Correctness Checks ═══");
                    for (int i = 0; i < runIds.size(); i++) {
                        for (int j = i + 1; j < runIds.size(); j++) {
                            DatabaseManager.runCorrectnessCheck(runIds.get(i), runIds.get(j));
                        }
                    }
                }
            } else {
                String runId = null;
                switch (choice) {
                    case "1": runId = MongoPipeline.runPipeline(files, batchSize, strategy, selectedQueries); break;
                    case "2": runId = MapReducePipeline.runPipeline(files, batchSize, strategy, selectedQueries); break;
                    case "3": runId = PigPipeline.runPipeline(files, batchSize, strategy, selectedQueries); break;
                    case "4": runId = HivePipeline.runPipeline(files, batchSize, strategy, selectedQueries); break;
                }
                if (runId != null) {
                    System.out.println("\nGenerating report...");
                    DatabaseManager.printReport(runId);
                }
            }

        } else if (choice.equals("6")) {
            System.out.print("Enter Run ID: ");
            String runId = scanner.nextLine().trim();
            DatabaseManager.printReport(runId);
        } else {
            System.out.println("Invalid choice.");
        }
    }
}
