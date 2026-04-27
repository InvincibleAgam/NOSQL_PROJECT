"""
Centralized configuration for NASA HTTP Log Analysis Tool.
All pipelines share these settings for consistency.
"""

import os

# ─── Data Paths ───────────────────────────────────────────────────────────────
DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "raw")
JULY_LOG = os.path.join(DATA_DIR, "NASA_access_log_Jul95")
AUGUST_LOG = os.path.join(DATA_DIR, "NASA_access_log_Aug95")

# ─── Batching ─────────────────────────────────────────────────────────────────
DEFAULT_BATCH_SIZE = 10000

# ─── MongoDB ──────────────────────────────────────────────────────────────────
MONGO_URI = "mongodb://localhost:27017"
MONGO_DB = "nasa_log_analysis"
MONGO_COLLECTION = "access_logs"

# ─── MySQL ────────────────────────────────────────────────────────────────────
MYSQL_HOST = "localhost"
MYSQL_PORT = 3307
MYSQL_USER = "root"
MYSQL_PASSWORD = ""           # default Homebrew MySQL has no password
MYSQL_DATABASE = "nasa_log_analysis"

# ─── Hadoop / MapReduce ──────────────────────────────────────────────────────
HADOOP_HOME = os.environ.get("HADOOP_HOME", "/opt/homebrew/opt/hadoop/libexec")
HADOOP_BIN = os.path.join(HADOOP_HOME, "..", "bin", "hadoop")
MR_JAR_DIR = os.path.join(os.path.dirname(__file__), "src", "pipelines", "mapreduce")
MR_JAR_NAME = "nasa-mr.jar"
MR_OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "data", "mr_output")

# ─── Supported Pipelines ─────────────────────────────────────────────────────
PIPELINES = {
    "mongodb":    "MongoDB Aggregation Pipeline",
    "mapreduce":  "Hadoop MapReduce",
    "pig":        "Apache Pig (Phase 2)",
    "hive":       "Apache Hive (Phase 2)",
}
