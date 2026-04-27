"""
MongoDB Aggregation Pipeline.

Implements the full ETL flow using MongoDB:
  1. Extract: Read raw log files in batches
  2. Transform: Parse lines using shared parser, insert into MongoDB
  3. Aggregate: Run 3 MongoDB aggregation pipelines (equivalent to the 3 SQL queries)
  4. Load: Write aggregated results to MySQL

This pipeline uses PyMongo for all MongoDB operations.
"""

import sys
import os
import time
import uuid
from datetime import datetime
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import config
from src.parser import parse_log_line
from src.batch_processor import batch_generator, compute_batch_stats
from src import loader


def run_mongodb_pipeline(filepaths, batch_size=None):
    """
    Execute the complete MongoDB ETL pipeline.

    Args:
        filepaths: list of log file paths
        batch_size: records per batch (default from config)

    Returns:
        run_id: identifier for this run
    """
    import pymongo

    batch_size = batch_size or config.DEFAULT_BATCH_SIZE
    run_id = f"mongo_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    pipeline_name = "mongodb"

    print("\n" + "=" * 70)
    print(f"  🍃  MongoDB Pipeline — Run ID: {run_id}")
    print(f"      Batch size: {batch_size:,}")
    print("=" * 70)

    # ── Connect to MongoDB ────────────────────────────────────────────────
    client = pymongo.MongoClient(config.MONGO_URI)
    db = client[config.MONGO_DB]
    collection = db[config.MONGO_COLLECTION]

    # Drop previous data for a clean run
    collection.drop()
    print("  ✓  MongoDB collection reset")

    # ── Setup MySQL ───────────────────────────────────────────────────────
    loader.create_database_and_tables()

    # ── Start timing (from file read start) ───────────────────────────────
    started_at = datetime.now()
    start_time = time.time()

    # ── Extract & Transform: Parse and insert into MongoDB ────────────────
    total_records = 0
    malformed_count = 0
    num_batches = 0
    last_batch_id = 0

    print("  ⏳ Processing log files...")

    for batch_id, lines in batch_generator(filepaths, batch_size):
        docs = []
        batch_malformed = 0

        for line in lines:
            parsed = parse_log_line(line)
            if parsed is None:
                batch_malformed += 1
            else:
                docs.append(parsed)

        total_records += len(lines)
        malformed_count += batch_malformed
        num_batches += 1
        last_batch_id = batch_id

        # Bulk insert parsed documents into MongoDB
        if docs:
            collection.insert_many(docs, ordered=False)

        if batch_id % 50 == 0:
            print(f"      Batch {batch_id}: {len(lines):,} lines ({batch_malformed} malformed)")

    print(f"  ✓  Inserted {total_records - malformed_count:,} docs "
          f"({malformed_count:,} malformed, {num_batches} batches)")

    # ── Aggregate: Run 3 queries via MongoDB aggregation ──────────────────
    execution_time = datetime.now()

    # ─── Query 1: Daily Traffic Summary ───────────────────────────────────
    print("  ⏳ Running Query 1 — Daily Traffic Summary...")
    q1_pipeline = [
        {"$group": {
            "_id": {"log_date": "$log_date", "status_code": "$status_code"},
            "request_count": {"$sum": 1},
            "total_bytes": {"$sum": "$bytes_transferred"},
        }},
        {"$project": {
            "_id": 0,
            "log_date": "$_id.log_date",
            "status_code": "$_id.status_code",
            "request_count": 1,
            "total_bytes": 1,
        }},
        {"$sort": {"log_date": 1, "status_code": 1}},
    ]
    q1_results = list(collection.aggregate(q1_pipeline, allowDiskUse=True))
    print(f"      → {len(q1_results)} rows")

    # Load into MySQL
    loader.load_query1_results(run_id, pipeline_name, last_batch_id, execution_time, q1_results)

    # ─── Query 2: Top 20 Requested Resources ─────────────────────────────
    print("  ⏳ Running Query 2 — Top 20 Requested Resources...")
    q2_pipeline = [
        {"$group": {
            "_id": "$resource_path",
            "request_count": {"$sum": 1},
            "total_bytes": {"$sum": "$bytes_transferred"},
            "distinct_hosts": {"$addToSet": "$host"},
        }},
        {"$project": {
            "_id": 0,
            "resource_path": "$_id",
            "request_count": 1,
            "total_bytes": 1,
            "distinct_host_count": {"$size": "$distinct_hosts"},
        }},
        {"$sort": {"request_count": -1}},
        {"$limit": 20},
    ]
    q2_results = list(collection.aggregate(q2_pipeline, allowDiskUse=True))
    print(f"      → {len(q2_results)} rows")

    loader.load_query2_results(run_id, pipeline_name, last_batch_id, execution_time, q2_results)

    # ─── Query 3: Hourly Error Analysis ───────────────────────────────────
    print("  ⏳ Running Query 3 — Hourly Error Analysis...")
    q3_pipeline = [
        {"$group": {
            "_id": {"log_date": "$log_date", "log_hour": "$log_hour"},
            "total_request_count": {"$sum": 1},
            "error_request_count": {
                "$sum": {"$cond": [
                    {"$and": [
                        {"$gte": ["$status_code", 400]},
                        {"$lte": ["$status_code", 599]},
                    ]},
                    1, 0
                ]}
            },
            "error_hosts": {
                "$addToSet": {
                    "$cond": [
                        {"$and": [
                            {"$gte": ["$status_code", 400]},
                            {"$lte": ["$status_code", 599]},
                        ]},
                        "$host",
                        "$$REMOVE"
                    ]
                }
            },
        }},
        {"$project": {
            "_id": 0,
            "log_date": "$_id.log_date",
            "log_hour": "$_id.log_hour",
            "error_request_count": 1,
            "total_request_count": 1,
            "error_rate": {
                "$cond": [
                    {"$eq": ["$total_request_count", 0]},
                    0,
                    {"$divide": ["$error_request_count", "$total_request_count"]},
                ]
            },
            "distinct_error_hosts": {"$size": "$error_hosts"},
        }},
        {"$sort": {"log_date": 1, "log_hour": 1}},
    ]
    q3_results = list(collection.aggregate(q3_pipeline, allowDiskUse=True))
    print(f"      → {len(q3_results)} rows")

    loader.load_query3_results(run_id, pipeline_name, last_batch_id, execution_time, q3_results)

    # ── Stop timing ───────────────────────────────────────────────────────
    end_time = time.time()
    completed_at = datetime.now()
    runtime_seconds = end_time - start_time

    # ── Store run metadata ────────────────────────────────────────────────
    avg_batch_size = compute_batch_stats(total_records, num_batches)
    loader.load_run_metadata(
        run_id, pipeline_name, batch_size, total_records,
        malformed_count, num_batches, avg_batch_size,
        runtime_seconds, started_at, completed_at,
    )

    print(f"\n  ✅  MongoDB Pipeline Complete")
    print(f"      Runtime: {runtime_seconds:.2f}s")
    print(f"      Records: {total_records:,} total, {malformed_count:,} malformed")
    print(f"      Batches: {num_batches} (avg size: {avg_batch_size:.1f})")

    # Cleanup
    client.close()

    return run_id
