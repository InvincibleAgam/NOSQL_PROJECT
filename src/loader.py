"""
MySQL Result Loader.

Loads aggregated query results into MySQL tables. Handles connection management,
table creation, and batch inserts with pipeline metadata.
"""

import mysql.connector
from datetime import datetime
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config


def get_connection():
    """Create a MySQL connection using config settings."""
    return mysql.connector.connect(
        host=config.MYSQL_HOST,
        port=config.MYSQL_PORT,
        user=config.MYSQL_USER,
        password=config.MYSQL_PASSWORD,
        database=config.MYSQL_DATABASE,
        autocommit=True,
    )


def create_database_and_tables():
    """Create the database and all tables if they don't exist."""
    # First connect without database to create it
    conn = mysql.connector.connect(
        host=config.MYSQL_HOST,
        port=config.MYSQL_PORT,
        user=config.MYSQL_USER,
        password=config.MYSQL_PASSWORD,
        autocommit=True,
    )
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {config.MYSQL_DATABASE}")
    cursor.close()
    conn.close()

    # Now connect to the database and run schema
    conn = get_connection()
    cursor = conn.cursor()

    schema_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "sql", "schema.sql")
    with open(schema_path, 'r') as f:
        lines = f.readlines()

    # Strip comment-only lines and blank lines, then rejoin
    clean_lines = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith('--') or not stripped:
            continue
        clean_lines.append(line)
    sql_script = ''.join(clean_lines)

    # Execute each statement separately
    for statement in sql_script.split(';'):
        stmt = statement.strip()
        if not stmt:
            continue
        # Skip USE and CREATE DATABASE (we handle these above)
        upper = stmt.upper()
        if upper.startswith('USE') or upper.startswith('CREATE DATABASE'):
            continue
        try:
            cursor.execute(stmt)
        except mysql.connector.Error as e:
            # Skip "already exists" type errors
            if e.errno not in (1050, 1007):  # Table/DB already exists
                print(f"  ⚠  SQL warning: {e}")

    cursor.close()
    conn.close()
    print("  ✓  MySQL tables ready")


def load_query1_results(run_id, pipeline_name, batch_id, execution_time, results):
    """
    Load Query 1 (Daily Traffic Summary) results into MySQL.

    Args:
        results: list of dicts with keys: log_date, status_code, request_count, total_bytes
    """
    conn = get_connection()
    cursor = conn.cursor()
    insert_sql = """
        INSERT INTO query1_results
            (pipeline_name, run_id, batch_id, execution_time, log_date, status_code, request_count, total_bytes)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    rows = [
        (pipeline_name, run_id, batch_id, execution_time,
         r["log_date"], r["status_code"], r["request_count"], r["total_bytes"])
        for r in results
    ]
    cursor.executemany(insert_sql, rows)
    conn.commit()
    cursor.close()
    conn.close()


def load_query2_results(run_id, pipeline_name, batch_id, execution_time, results):
    """
    Load Query 2 (Top Requested Resources) results into MySQL.

    Args:
        results: list of dicts with keys: resource_path, request_count, total_bytes, distinct_host_count
    """
    conn = get_connection()
    cursor = conn.cursor()
    insert_sql = """
        INSERT INTO query2_results
            (pipeline_name, run_id, batch_id, execution_time, resource_path, request_count, total_bytes, distinct_host_count)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    rows = [
        (pipeline_name, run_id, batch_id, execution_time,
         r["resource_path"][:512], r["request_count"], r["total_bytes"], r["distinct_host_count"])
        for r in results
    ]
    cursor.executemany(insert_sql, rows)
    conn.commit()
    cursor.close()
    conn.close()


def load_query3_results(run_id, pipeline_name, batch_id, execution_time, results):
    """
    Load Query 3 (Hourly Error Analysis) results into MySQL.

    Args:
        results: list of dicts with keys: log_date, log_hour, error_request_count,
                 total_request_count, error_rate, distinct_error_hosts
    """
    conn = get_connection()
    cursor = conn.cursor()
    insert_sql = """
        INSERT INTO query3_results
            (pipeline_name, run_id, batch_id, execution_time, log_date, log_hour,
             error_request_count, total_request_count, error_rate, distinct_error_hosts)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    rows = [
        (pipeline_name, run_id, batch_id, execution_time,
         r["log_date"], r["log_hour"], r["error_request_count"],
         r["total_request_count"], r["error_rate"], r["distinct_error_hosts"])
        for r in results
    ]
    cursor.executemany(insert_sql, rows)
    conn.commit()
    cursor.close()
    conn.close()


def load_run_metadata(run_id, pipeline_name, batch_size, total_records,
                      malformed_records, num_batches, avg_batch_size,
                      runtime_seconds, started_at, completed_at):
    """Store run-level metadata."""
    conn = get_connection()
    cursor = conn.cursor()
    insert_sql = """
        INSERT INTO run_metadata
            (run_id, pipeline_name, batch_size, total_records, malformed_records,
             num_batches, avg_batch_size, runtime_seconds, started_at, completed_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.execute(insert_sql, (
        run_id, pipeline_name, batch_size, total_records, malformed_records,
        num_batches, round(avg_batch_size, 2), round(runtime_seconds, 3),
        started_at, completed_at
    ))
    conn.commit()
    cursor.close()
    conn.close()
