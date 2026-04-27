package com.invincibleagam;

import com.invincibleagam.core.Config;
import com.invincibleagam.core.DatabaseManager;
import com.invincibleagam.pipelines.MapReducePipeline;
import com.invincibleagam.pipelines.MongoPipeline;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

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
        System.out.print("\nChoice [1-3]: ");

        String choice = scanner.nextLine().trim();

        if (choice.equals("1") || choice.equals("2")) {
            System.out.print("Batch size [" + Config.DEFAULT_BATCH_SIZE + "]: ");
            String batchInput = scanner.nextLine().trim();
            int batchSize = Config.DEFAULT_BATCH_SIZE;
            if (!batchInput.isEmpty()) {
                try {
                    batchSize = Integer.parseInt(batchInput);
                } catch (Exception ignored) {}
            }

            String runId = null;
            if (choice.equals("1")) {
                runId = MongoPipeline.runPipeline(files, batchSize);
            } else {
                runId = MapReducePipeline.runPipeline(files, batchSize);
            }

            if (runId != null) {
                System.out.println("\nGenerating report...");
                DatabaseManager.printReport(runId);
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
