package com.invincibleagam;

import com.invincibleagam.core.BatchStrategy;
import com.invincibleagam.core.Config;
import com.invincibleagam.core.DatabaseManager;
import com.invincibleagam.pipelines.MapReducePipeline;
import com.invincibleagam.pipelines.MongoPipeline;

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
        System.out.println("\nSelect an action:");
        System.out.println("  1. Run MongoDB Pipeline");
        System.out.println("  2. Run MapReduce Pipeline (Hadoop native)");
        System.out.println("  3. View Report for a Run");
        System.out.println("  4. Run Both Pipelines & Correctness Check");
        System.out.print("\nChoice [1-4]: ");

        String choice = scanner.nextLine().trim();

        if (choice.equals("1") || choice.equals("2") || choice.equals("4")) {

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
            if (choice.equals("4")) {
                // Run both pipelines and then correctness check
                System.out.println("\n  Running MongoDB Pipeline first...");
                String mongoRunId = MongoPipeline.runPipeline(files, batchSize, strategy, selectedQueries);

                System.out.println("\n  Running MapReduce Pipeline second...");
                String mrRunId = MapReducePipeline.runPipeline(files, batchSize, strategy, selectedQueries);

                if (mongoRunId != null && mrRunId != null) {
                    System.out.println("\nGenerating reports...");
                    DatabaseManager.printReport(mongoRunId);
                    DatabaseManager.printReport(mrRunId);

                    // Correctness check
                    DatabaseManager.runCorrectnessCheck(mongoRunId, mrRunId);
                }
            } else {
                String runId = null;
                if (choice.equals("1")) {
                    runId = MongoPipeline.runPipeline(files, batchSize, strategy, selectedQueries);
                } else {
                    runId = MapReducePipeline.runPipeline(files, batchSize, strategy, selectedQueries);
                }

                if (runId != null) {
                    System.out.println("\nGenerating report...");
                    DatabaseManager.printReport(runId);
                }
            }

        } else if (choice.equals("3")) {
            System.out.print("Enter Run ID: ");
            String runId = scanner.nextLine().trim();
            DatabaseManager.printReport(runId);
        } else {
            System.out.println("Invalid choice.");
        }
    }
}
