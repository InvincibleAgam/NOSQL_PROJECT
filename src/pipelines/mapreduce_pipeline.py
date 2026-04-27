"""
Hadoop MapReduce Pipeline.

Implements the full ETL flow using native Java Hadoop MapReduce:
  1. Extract & Transform: Python reads logs, parses via regex, and writes a normalized TSV for Hadoop.
  2. Aggregate: Compiles and executes Java MapReduce Jobs via 'hadoop jar' natively.
  3. Load: Reads Hadoop output and writes aggregated results to MySQL.
"""

import sys
import os
import time
import uuid
import subprocess
import shutil
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import config
from src.parser import parse_log_line_tuple
from src.batch_processor import batch_generator, compute_batch_stats
from src import loader

# Paths
MR_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mapreduce")
JAVA_SRC = os.path.join(MR_DIR, "NASALogDriver.java")
CLASSES_DIR = os.path.join(MR_DIR, "classes")
JAR_FILE = os.path.join(MR_DIR, "nasa-mr.jar")
INPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data", "mr_input")
INPUT_FILE = os.path.join(INPUT_DIR, "mr_input.tsv")
OUTPUT_BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data", "mr_output")


def compile_java():
    """Compile the Java MapReduce driver into a JAR."""
    print("  ⏳ Compiling Java MapReduce classes...")
    os.makedirs(CLASSES_DIR, exist_ok=True)
    
    hadoop_classpath = subprocess.check_output(["hadoop", "classpath"]).decode("utf-8").strip()
    
    compile_cmd = ["javac", "--release", "17", "-cp", hadoop_classpath, "-d", CLASSES_DIR, JAVA_SRC]
    subprocess.run(compile_cmd, check=True)
    
    jar_cmd = ["jar", "-cvf", JAR_FILE, "-C", CLASSES_DIR, "."]
    subprocess.run(jar_cmd, check=True, stdout=subprocess.DEVNULL)
    print("  ✓  Java compilation successful")


def run_hadoop_job(query_id, output_dir):
    """Execute the compiled Java MR job via Hadoop."""
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
        
    cmd = [
        "hadoop", "jar", JAR_FILE, "mapreduce.NASALogDriver", query_id, INPUT_DIR, output_dir
    ]
    print(f"      Running Hadoop Job for {query_id}...")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Hadoop job failed:\n{result.stderr}")


def read_hadoop_output(output_dir):
    """Read the part-r-00000 output file from Hadoop."""
    output_file = os.path.join(output_dir, "part-r-00000")
    if not os.path.exists(output_file):
        return []
    with open(output_file, 'r') as f:
        return [line.strip() for line in f if line.strip()]


def run_mapreduce_pipeline(filepaths, batch_size=None):
    batch_size = batch_size or config.DEFAULT_BATCH_SIZE
    run_id = f"mr_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    pipeline_name = "mapreduce"

    print("\n" + "=" * 70)
    print(f"  🗺️  MapReduce Pipeline (Java Native) — Run ID: {run_id}")
    print(f"      Batch size: {batch_size:,}")
    print("=" * 70)

    loader.create_database_and_tables()

    # Compile Java Code
    compile_java()

    started_at = datetime.now()
    start_time = time.time()

    # 1. Parse and stage data for Hadoop Input
    print("  ⏳ Parsing log files & staging for Hadoop...")
    os.makedirs(INPUT_DIR, exist_ok=True)
    
    total_records = 0
    malformed_count = 0
    num_batches = 0
    last_batch_id = 0

    with open(INPUT_FILE, "w", encoding="utf-8") as f_out:
        for batch_id, lines in batch_generator(filepaths, batch_size):
            for line in lines:
                parsed = parse_log_line_tuple(line)
                if parsed is None:
                    malformed_count += 1
                else:
                    f_out.write("\t".join(str(p) for p in parsed) + "\n")
                    
            total_records += len(lines)
            num_batches += 1
            last_batch_id = batch_id
            
            if batch_id % 50 == 0:
                print(f"      Batch {batch_id}: {len(lines):,} lines processed")

    print(f"  ✓  Staged {total_records - malformed_count:,} records for Hadoop "
          f"({malformed_count:,} malformed, {num_batches} batches)")

    execution_time = datetime.now()

    # ─── Query 1: Daily Traffic Summary ───────────────────────────────────
    print("  ⏳ Running Query 1 (Hadoop)...")
    out_q1 = os.path.join(OUTPUT_BASE_DIR, "q1")
    run_hadoop_job("q1", out_q1)
    
    q1_results = []
    for line in read_hadoop_output(out_q1):
        parts = line.split("\t")
        q1_results.append({
            "log_date": parts[0],
            "status_code": int(parts[1]),
            "request_count": int(parts[2]),
            "total_bytes": int(parts[3])
        })
    print(f"      → {len(q1_results)} rows loaded to MySQL")
    loader.load_query1_results(run_id, pipeline_name, last_batch_id, execution_time, q1_results)


    # ─── Query 2: Top Requested Resources ──────────────────────────────────
    print("  ⏳ Running Query 2 (Hadoop)...")
    out_q2 = os.path.join(OUTPUT_BASE_DIR, "q2")
    run_hadoop_job("q2", out_q2)
    
    q2_results_all = []
    for line in read_hadoop_output(out_q2):
        parts = line.split("\t")
        q2_results_all.append({
            "resource_path": parts[0],
            "request_count": int(parts[1]),
            "total_bytes": int(parts[2]),
            "distinct_host_count": int(parts[3])
        })
    
    # Sort and take top 20 (Simulating a second Hadoop Job for Top N)
    q2_results = sorted(q2_results_all, key=lambda x: x["request_count"], reverse=True)[:20]
    print(f"      → {len(q2_results)} rows loaded to MySQL")
    loader.load_query2_results(run_id, pipeline_name, last_batch_id, execution_time, q2_results)


    # ─── Query 3: Hourly Error Analysis ────────────────────────────────────
    print("  ⏳ Running Query 3 (Hadoop)...")
    out_q3 = os.path.join(OUTPUT_BASE_DIR, "q3")
    run_hadoop_job("q3", out_q3)
    
    q3_results = []
    for line in read_hadoop_output(out_q3):
        parts = line.split("\t")
        q3_results.append({
            "log_date": parts[0],
            "log_hour": int(parts[1]),
            "error_request_count": int(parts[2]),
            "total_request_count": int(parts[3]),
            "error_rate": float(parts[4]),
            "distinct_error_hosts": int(parts[5])
        })
    print(f"      → {len(q3_results)} rows loaded to MySQL")
    loader.load_query3_results(run_id, pipeline_name, last_batch_id, execution_time, q3_results)


    # Cleanup Hadoop Dirs
    shutil.rmtree(INPUT_DIR, ignore_errors=True)
    shutil.rmtree(OUTPUT_BASE_DIR, ignore_errors=True)

    # ── Stop timing ───────────────────────────────────────────────────────
    end_time = time.time()
    completed_at = datetime.now()
    runtime_seconds = end_time - start_time

    avg_batch_size = compute_batch_stats(total_records, num_batches)
    loader.load_run_metadata(
        run_id, pipeline_name, batch_size, total_records,
        malformed_count, num_batches, avg_batch_size,
        runtime_seconds, started_at, completed_at,
    )

    print(f"\n  ✅  Java MapReduce Pipeline Complete")
    print(f"      Runtime: {runtime_seconds:.2f}s")
    print(f"      Records: {total_records:,} total, {malformed_count:,} malformed")
    print(f"      Batches: {num_batches} (avg size: {avg_batch_size:.1f})")

    return run_id
