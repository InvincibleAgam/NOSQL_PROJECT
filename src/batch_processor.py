"""
Batch Processor for NASA HTTP Log files.

Reads log files and yields batches of raw lines. Handles .gz compressed files
transparently. Tracks batch IDs (starting from 1) and computes statistics.
"""

import gzip
import os


def open_log_file(filepath):
    """Open a log file, handling .gz compression transparently."""
    if filepath.endswith('.gz'):
        return gzip.open(filepath, 'rt', encoding='latin-1', errors='replace')
    else:
        return open(filepath, 'r', encoding='latin-1', errors='replace')


def read_lines_from_files(filepaths):
    """
    Generator that yields one line at a time from a list of log files.
    Handles both plain text and .gz files.
    """
    for filepath in filepaths:
        if not os.path.exists(filepath):
            print(f"  ⚠  File not found: {filepath}")
            continue
        with open_log_file(filepath) as f:
            for line in f:
                stripped = line.strip()
                if stripped:
                    yield stripped


def batch_generator(filepaths, batch_size):
    """
    Generator that yields batches of log lines.

    Args:
        filepaths: list of log file paths (plain or .gz)
        batch_size: number of lines per batch

    Yields:
        (batch_id, lines) where batch_id starts at 1 and lines is a list
        of raw log line strings. The last batch may have fewer than batch_size lines.
    """
    batch_id = 1
    batch = []
    for line in read_lines_from_files(filepaths):
        batch.append(line)
        if len(batch) >= batch_size:
            yield (batch_id, batch)
            batch_id += 1
            batch = []
    # Final partial batch
    if batch:
        yield (batch_id, batch)


def compute_batch_stats(total_records, num_batches):
    """
    Compute average batch size as specified in the project:
      avg_batch_size = total_records / number_of_non_empty_batches
    """
    if num_batches == 0:
        return 0.0
    return total_records / num_batches
