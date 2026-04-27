package com.invincibleagam.core;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class BatchProcessor implements Iterable<List<String>> {
    private final List<String> filepaths;
    private final int batchSize;

    public BatchProcessor(List<String> filepaths, int batchSize) {
        this.filepaths = filepaths;
        this.batchSize = batchSize;
    }

    @Override
    public Iterator<List<String>> iterator() {
        return new BatchIterator();
    }

    private class BatchIterator implements Iterator<List<String>> {
        private int currentFileIndex = 0;
        private BufferedReader currentReader = null;
        private List<String> nextBatch = null;

        public BatchIterator() {
            advanceReader();
            prepareNextBatch();
        }

        private void advanceReader() {
            try {
                if (currentReader != null) {
                    currentReader.close();
                }
            } catch (IOException ignored) {}

            while (currentFileIndex < filepaths.size()) {
                String path = filepaths.get(currentFileIndex++);
                File f = new File(path);
                if (f.exists()) {
                    try {
                        InputStream fileStream = new FileInputStream(f);
                        if (path.endsWith(".gz")) {
                            InputStream gzipStream = new GZIPInputStream(fileStream);
                            Reader decoder = new InputStreamReader(gzipStream, "ISO-8859-1");
                            currentReader = new BufferedReader(decoder);
                        } else {
                            Reader decoder = new InputStreamReader(fileStream, "ISO-8859-1");
                            currentReader = new BufferedReader(decoder);
                        }
                        return; // Successfully opened a file
                    } catch (IOException e) {
                        System.err.println("Error opening file: " + path);
                    }
                }
            }
            currentReader = null;
        }

        private void prepareNextBatch() {
            nextBatch = new ArrayList<>(batchSize);
            while (currentReader != null && nextBatch.size() < batchSize) {
                try {
                    String line = currentReader.readLine();
                    if (line != null) {
                        nextBatch.add(line);
                    } else {
                        advanceReader(); // EOF, move to next file
                    }
                } catch (IOException e) {
                    advanceReader(); // Error reading, move to next
                }
            }
            if (nextBatch.isEmpty()) {
                nextBatch = null;
            }
        }

        @Override
        public boolean hasNext() {
            return nextBatch != null;
        }

        @Override
        public List<String> next() {
            List<String> toReturn = nextBatch;
            prepareNextBatch();
            return toReturn;
        }
    }
}
