#!/usr/bin/env python3
"""
NASA HTTP Log Analysis Tool — Main Controller

DAS 839 NoSQL Systems End-Term Project
Phase 1: MongoDB + MapReduce pipelines

Usage:
    python3 main.py                         # Interactive menu
    python3 main.py --pipeline mongodb      # Run specific pipeline
    python3 main.py --pipeline mapreduce    # Run MapReduce
    python3 main.py --report <run_id>       # View report for a run
    python3 main.py --list-runs             # List all runs
"""

import argparse
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config


def check_data_files():
    """Check if NASA log files exist and return available file paths."""
    files = []
    for path in [config.JULY_LOG, config.AUGUST_LOG]:
        if os.path.exists(path):
            size_mb = os.path.getsize(path) / (1024 * 1024)
            print(f"  ✓  Found: {os.path.basename(path)} ({size_mb:.1f} MB)")
            files.append(path)
        else:
            # Check for .gz version
            gz_path = path + ".gz"
            if os.path.exists(gz_path):
                size_mb = os.path.getsize(gz_path) / (1024 * 1024)
                print(f"  ✓  Found: {os.path.basename(gz_path)} ({size_mb:.1f} MB)")
                files.append(gz_path)
            else:
                print(f"  ✗  Missing: {os.path.basename(path)}")

    if not files:
        print("\n  ⚠  No data files found!")
        print("  Run: bash scripts/download_data.sh")
        print("  Or place NASA log files in: data/raw/")
        sys.exit(1)

    return files


def interactive_menu():
    """Display the interactive pipeline selection menu."""
    print("\n" + "═" * 70)
    print("  🚀  NASA HTTP Log Analysis Tool")
    print("  DAS 839 — NoSQL Systems End-Term Project")
    print("═" * 70)
    print()

    # Check data files
    print("  📂 Data Files:")
    files = check_data_files()
    print()

    # Pipeline selection
    print("  Select a pipeline:")
    print("  ─────────────────────────────────────────")
    print("  [1]  MongoDB Aggregation Pipeline")
    print("  [2]  Hadoop MapReduce")
    print("  [3]  Apache Pig          (Phase 2 — not yet implemented)")
    print("  [4]  Apache Hive         (Phase 2 — not yet implemented)")
    print("  ─────────────────────────────────────────")
    print("  [5]  View report for a previous run")
    print("  [6]  List all runs")
    print("  [0]  Exit")
    print()

    choice = input("  Enter choice [1-6]: ").strip()

    if choice == '1':
        run_pipeline("mongodb", files)
    elif choice == '2':
        run_pipeline("mapreduce", files)
    elif choice == '3':
        print("\n  ⚠  Apache Pig is planned for Phase 2.")
    elif choice == '4':
        print("\n  ⚠  Apache Hive is planned for Phase 2.")
    elif choice == '5':
        run_id = input("  Enter run ID: ").strip()
        view_report(run_id)
    elif choice == '6':
        list_runs()
    elif choice == '0':
        print("  Bye!")
        sys.exit(0)
    else:
        print("  ⚠  Invalid choice.")


def run_pipeline(pipeline_name, files):
    """Run the selected pipeline."""
    # Ask for batch size
    batch_input = input(f"\n  Batch size [{config.DEFAULT_BATCH_SIZE}]: ").strip()
    batch_size = int(batch_input) if batch_input else config.DEFAULT_BATCH_SIZE

    if pipeline_name == "mongodb":
        from src.pipelines.mongodb_pipeline import run_mongodb_pipeline
        run_id = run_mongodb_pipeline(files, batch_size)
    elif pipeline_name == "mapreduce":
        from src.pipelines.mapreduce_pipeline import run_mapreduce_pipeline
        run_id = run_mapreduce_pipeline(files, batch_size)
    else:
        print(f"  ⚠  Pipeline '{pipeline_name}' not implemented yet.")
        return

    # Show report
    print(f"\n  Run complete! Run ID: {run_id}")
    view_report(run_id)


def view_report(run_id):
    """View the report for a given run."""
    from src.reporter import display_full_report
    display_full_report(run_id)


def list_runs():
    """List all previous runs."""
    from src.reporter import display_run_metadata
    display_run_metadata()


def main():
    parser = argparse.ArgumentParser(
        description="NASA HTTP Log Analysis Tool — NoSQL Systems Project"
    )
    parser.add_argument(
        "--pipeline", "-p",
        choices=["mongodb", "mapreduce", "pig", "hive"],
        help="Pipeline to run (skips interactive menu)"
    )
    parser.add_argument(
        "--batch-size", "-b",
        type=int,
        default=config.DEFAULT_BATCH_SIZE,
        help=f"Batch size (default: {config.DEFAULT_BATCH_SIZE})"
    )
    parser.add_argument(
        "--report", "-r",
        metavar="RUN_ID",
        help="View report for a specific run"
    )
    parser.add_argument(
        "--list-runs", "-l",
        action="store_true",
        help="List all previous runs"
    )

    args = parser.parse_args()

    if args.report:
        view_report(args.report)
    elif args.list_runs:
        list_runs()
    elif args.pipeline:
        files = check_data_files()
        if args.pipeline in ("pig", "hive"):
            print(f"\n  ⚠  {args.pipeline.title()} is planned for Phase 2.")
            sys.exit(1)
        run_pipeline(args.pipeline, files)
    else:
        interactive_menu()


if __name__ == "__main__":
    main()
