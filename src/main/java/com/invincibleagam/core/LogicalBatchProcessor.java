package com.invincibleagam.core;

import com.invincibleagam.models.ParsedLog;

import java.io.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.IsoFields;
import java.util.*;
import java.util.zip.GZIPInputStream;

/**
 * Groups raw log lines into logical batches based on a BatchStrategy.
 * 
 * - FIXED:   same as the original BatchProcessor (N lines per batch)
 * - MONTHLY: groups by calendar month derived from the log timestamp
 * - WEEKLY:  groups by ISO week number derived from the log timestamp
 * 
 * For MONTHLY/WEEKLY, the batch key (e.g. "1995-07" or "1995-W27") is exposed
 * so that callers can record it in the batch_metadata table.
 */
public class LogicalBatchProcessor implements Iterable<Map.Entry<String, List<String>>> {

    private final List<String> filepaths;
    private final BatchStrategy strategy;
    private final int fixedBatchSize;

    public LogicalBatchProcessor(List<String> filepaths, BatchStrategy strategy, int fixedBatchSize) {
        this.filepaths = filepaths;
        this.strategy = strategy;
        this.fixedBatchSize = fixedBatchSize;
    }

    @Override
    public Iterator<Map.Entry<String, List<String>>> iterator() {
        if (strategy == BatchStrategy.FIXED) {
            return new FixedBatchIterator();
        } else {
            return new LogicalBatchIterator();
        }
    }

    // ── Helper: extract the batch key from a raw log line ────────────────────
    private String extractBatchKey(String line) {
        // Attempt to parse the date from the log line bracket: [01/Jul/1995:00:00:01 -0400]
        int openBracket = line.indexOf('[');
        int closeBracket = line.indexOf(']');
        if (openBracket < 0 || closeBracket < 0 || closeBracket <= openBracket) {
            return null; // malformed — cannot extract date
        }
        String tsStr = line.substring(openBracket + 1, closeBracket);
        // Format: dd/MMM/yyyy:HH:mm:ss Z
        try {
            String datePart = tsStr.split(":")[0]; // "01/Jul/1995"
            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("dd/MMM/yyyy", Locale.ENGLISH);
            LocalDate date = LocalDate.parse(datePart, dtf);

            if (strategy == BatchStrategy.MONTHLY) {
                return String.format("%04d-%02d", date.getYear(), date.getMonthValue());
            } else { // WEEKLY
                int weekNumber = date.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);
                int weekYear = date.get(IsoFields.WEEK_BASED_YEAR);
                return String.format("%04d-W%02d", weekYear, weekNumber);
            }
        } catch (Exception e) {
            return null;
        }
    }

    // ── Fixed-size batch iterator (wraps existing logic) ────────────────────
    private class FixedBatchIterator implements Iterator<Map.Entry<String, List<String>>> {
        private int currentFileIndex = 0;
        private BufferedReader currentReader = null;
        private Map.Entry<String, List<String>> nextBatch = null;
        private int batchCounter = 0;

        FixedBatchIterator() {
            advanceReader();
            prepareNextBatch();
        }

        private void advanceReader() {
            try { if (currentReader != null) currentReader.close(); } catch (IOException ignored) {}
            while (currentFileIndex < filepaths.size()) {
                String path = filepaths.get(currentFileIndex++);
                File f = new File(path);
                if (f.exists()) {
                    try {
                        InputStream fs = new FileInputStream(f);
                        if (path.endsWith(".gz")) {
                            fs = new GZIPInputStream(fs);
                        }
                        currentReader = new BufferedReader(new InputStreamReader(fs, "ISO-8859-1"));
                        return;
                    } catch (IOException e) {
                        System.err.println("Error opening file: " + path);
                    }
                }
            }
            currentReader = null;
        }

        private void prepareNextBatch() {
            List<String> batch = new ArrayList<>(fixedBatchSize);
            while (currentReader != null && batch.size() < fixedBatchSize) {
                try {
                    String line = currentReader.readLine();
                    if (line != null) { batch.add(line); }
                    else { advanceReader(); }
                } catch (IOException e) { advanceReader(); }
            }
            if (batch.isEmpty()) {
                nextBatch = null;
            } else {
                batchCounter++;
                nextBatch = new AbstractMap.SimpleEntry<>("batch_" + batchCounter, batch);
            }
        }

        @Override public boolean hasNext() { return nextBatch != null; }
        @Override public Map.Entry<String, List<String>> next() {
            Map.Entry<String, List<String>> toReturn = nextBatch;
            prepareNextBatch();
            return toReturn;
        }
    }

    // ── Logical (month/week) batch iterator ─────────────────────────────────
    private class LogicalBatchIterator implements Iterator<Map.Entry<String, List<String>>> {
        /** Pre-built ordered list of (batchKey → lines) */
        private final Iterator<Map.Entry<String, List<String>>> inner;

        LogicalBatchIterator() {
            // We must read all lines to group them. For NASA datasets (~3.5M lines) this is fine.
            LinkedHashMap<String, List<String>> groups = new LinkedHashMap<>();
            for (String path : filepaths) {
                File f = new File(path);
                if (!f.exists()) continue;
                try (BufferedReader br = new BufferedReader(
                        new InputStreamReader(
                                path.endsWith(".gz") ? new GZIPInputStream(new FileInputStream(f))
                                        : new FileInputStream(f), "ISO-8859-1"))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String key = extractBatchKey(line);
                        if (key == null) key = "__malformed__";
                        groups.computeIfAbsent(key, k -> new ArrayList<>()).add(line);
                    }
                } catch (IOException e) {
                    System.err.println("Error reading file: " + path);
                }
            }
            inner = groups.entrySet().iterator();
        }

        @Override public boolean hasNext() { return inner.hasNext(); }
        @Override public Map.Entry<String, List<String>> next() { return inner.next(); }
    }
}
