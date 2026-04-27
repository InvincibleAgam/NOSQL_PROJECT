"""
Reporting Module.

Reads query results and run metadata from MySQL and displays them in a
clear, formatted table. Shows all required execution metadata.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config


def get_connection():
    """Create a MySQL connection."""
    import mysql.connector
    return mysql.connector.connect(
        host=config.MYSQL_HOST,
        port=config.MYSQL_PORT,
        user=config.MYSQL_USER,
        password=config.MYSQL_PASSWORD,
        database=config.MYSQL_DATABASE,
        autocommit=True,
    )


def _format_table(headers, rows, col_widths=None):
    """Simple table formatter (no external dependencies)."""
    if not rows:
        return "  (no data)\n"

    # Compute column widths
    if col_widths is None:
        col_widths = [len(h) for h in headers]
        for row in rows:
            for i, val in enumerate(row):
                col_widths[i] = max(col_widths[i], len(str(val)))

    # Header
    line = "  "
    hdr_line = "  "
    for i, h in enumerate(headers):
        line += f"│ {h:<{col_widths[i]}} "
        hdr_line += f"├{'─' * (col_widths[i] + 2)}"
    line += "│\n"
    hdr_line += "┤\n"

    # Top border
    top = "  ┌" + "┬".join("─" * (w + 2) for w in col_widths) + "┐\n"
    bottom = "  └" + "┴".join("─" * (w + 2) for w in col_widths) + "┘\n"

    output = top + line + hdr_line

    # Rows
    for row in rows:
        row_line = "  "
        for i, val in enumerate(row):
            row_line += f"│ {str(val):<{col_widths[i]}} "
        row_line += "│\n"
        output += row_line

    output += bottom
    return output


def display_run_metadata(run_id=None, pipeline_name=None):
    """Display run metadata from the run_metadata table."""
    conn = get_connection()
    cursor = conn.cursor()

    if run_id:
        cursor.execute("SELECT * FROM run_metadata WHERE run_id = %s", (run_id,))
    elif pipeline_name:
        cursor.execute("SELECT * FROM run_metadata WHERE pipeline_name = %s ORDER BY started_at DESC LIMIT 5", (pipeline_name,))
    else:
        cursor.execute("SELECT * FROM run_metadata ORDER BY started_at DESC LIMIT 10")

    rows = cursor.fetchall()
    headers = ["ID", "Run ID", "Pipeline", "Batch Size", "Total Records", "Malformed",
               "# Batches", "Avg Batch Size", "Runtime (s)", "Started", "Completed"]

    print("\n" + "=" * 80)
    print("  📊  RUN METADATA")
    print("=" * 80)
    if rows:
        for row in rows:
            print(f"\n  Run ID     : {row[1]}")
            print(f"  Pipeline   : {row[2]}")
            print(f"  Batch Size : {row[3]}")
            print(f"  Total Recs : {row[4]:,}")
            print(f"  Malformed  : {row[5]:,}")
            print(f"  # Batches  : {row[6]}")
            print(f"  Avg Batch  : {row[7]}")
            print(f"  Runtime    : {row[8]}s")
            print(f"  Started    : {row[9]}")
            print(f"  Completed  : {row[10]}")
    else:
        print("  (no runs found)")

    cursor.close()
    conn.close()


def display_query1_results(run_id):
    """Display Query 1 — Daily Traffic Summary."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT log_date, status_code, request_count, total_bytes, batch_id
        FROM query1_results
        WHERE run_id = %s
        ORDER BY log_date, status_code
    """, (run_id,))
    rows = cursor.fetchall()

    print("\n" + "─" * 80)
    print("  📋  QUERY 1 — Daily Traffic Summary")
    print("─" * 80)
    print(f"  Rows: {len(rows)}")
    headers = ["log_date", "status_code", "request_count", "total_bytes", "batch_id"]
    # Show first 20 rows
    display_rows = rows[:20]
    print(_format_table(headers, display_rows))
    if len(rows) > 20:
        print(f"  ... and {len(rows) - 20} more rows")

    cursor.close()
    conn.close()


def display_query2_results(run_id):
    """Display Query 2 — Top Requested Resources."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT resource_path, request_count, total_bytes, distinct_host_count, batch_id
        FROM query2_results
        WHERE run_id = %s
        ORDER BY request_count DESC
    """, (run_id,))
    rows = cursor.fetchall()

    print("\n" + "─" * 80)
    print("  📋  QUERY 2 — Top 20 Requested Resources")
    print("─" * 80)
    headers = ["resource_path", "request_count", "total_bytes", "distinct_host_count", "batch_id"]
    # Truncate long paths for display
    display_rows = []
    for r in rows[:20]:
        path = r[0][:60] + "..." if len(r[0]) > 60 else r[0]
        display_rows.append((path, r[1], r[2], r[3], r[4]))
    print(_format_table(headers, display_rows))

    cursor.close()
    conn.close()


def display_query3_results(run_id):
    """Display Query 3 — Hourly Error Analysis."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT log_date, log_hour, error_request_count, total_request_count,
               error_rate, distinct_error_hosts, batch_id
        FROM query3_results
        WHERE run_id = %s
        ORDER BY log_date, log_hour
    """, (run_id,))
    rows = cursor.fetchall()

    print("\n" + "─" * 80)
    print("  📋  QUERY 3 — Hourly Error Analysis")
    print("─" * 80)
    print(f"  Rows: {len(rows)}")
    headers = ["log_date", "log_hour", "error_count", "total_count", "error_rate", "distinct_hosts", "batch_id"]
    display_rows = rows[:20]
    print(_format_table(headers, display_rows))
    if len(rows) > 20:
        print(f"  ... and {len(rows) - 20} more rows")

    cursor.close()
    conn.close()


def display_full_report(run_id):
    """Display the complete report for a given run."""
    display_run_metadata(run_id=run_id)
    display_query1_results(run_id)
    display_query2_results(run_id)
    display_query3_results(run_id)
    print("\n" + "=" * 80)
    print("  ✅  Report complete")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        display_full_report(sys.argv[1])
    else:
        display_run_metadata()
