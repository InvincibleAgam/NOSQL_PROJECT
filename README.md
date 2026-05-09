# NASA HTTP Log Analysis — DAS 839 NoSQL Systems End-Term Project

A unified **Java CLI tool** for comparative log analytics across **4 NoSQL/big-data execution pipelines**: MongoDB, MapReduce, Pig, and Hive. All pipelines process NASA HTTP access logs (July & August 1995, ~3.46M records) and produce mathematically identical results.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [System Architecture](#2-system-architecture)
3. [Repository Structure](#3-repository-structure)
4. [Prerequisites](#4-prerequisites)
5. [Setup & Installation](#5-setup--installation)
6. [Running the Project](#6-running-the-project)
7. [Pipelines](#7-pipelines)
8. [Mandatory Queries](#8-mandatory-queries)
9. [Parsing Strategy](#9-parsing-strategy)
10. [Batching Strategy](#10-batching-strategy)
11. [Database Schema](#11-database-schema)
12. [Correctness Check](#12-correctness-check)
13. [Phase History](#13-phase-history)

---

## 1. Project Overview

This project implements a **comparative systems prototype** (as specified in the DAS 839 End-Term guidelines) where the user selects one of four execution pipelines via a single CLI interface. Each pipeline performs the complete **ETL flow** — parse, clean, batch, aggregate, load, and report — using its native execution technology while sharing the same parsing rules, cleaning logic, and output schema.

**Key Features:**
- 🔄 **4 Pipelines**: MongoDB, MapReduce, Pig, Hive — switchable at runtime
- 📊 **3 Mandatory Queries**: Daily Traffic, Top Resources, Hourly Errors
- 📦 **3 Batching Strategies**: Fixed-size, Monthly, Weekly
- ✅ **Built-in Correctness Checker**: Pairwise comparison across all pipelines
- 🗄️ **MySQL Reporting**: Structured result tables with full run/batch metadata
- 🔢 **Malformed Record Tracking**: Per-batch and per-run counters (never silently dropped)

---

## 2. System Architecture

```
┌─────────────────────────────────────────────────────┐
│              Java CLI Controller                     │
│         (Main.java — pipeline selector)              │
│  Select: 1.MongoDB  2.MR  3.Pig  4.Hive  5.All+Check│
└──┬─────────┬─────────┬─────────┬────────────────────┘
   │         │         │         │
┌──▼───┐  ┌──▼───┐  ┌──▼───┐  ┌──▼───┐
│Mongo │  │  MR  │  │ Pig  │  │ Hive │
│Pipe  │  │ Pipe │  │ Pipe │  │ Pipe │
│.java │  │.java │  │.java │  │.java │
└──┬───┘  └──┬───┘  └──┬───┘  └──┬───┘
   │         │         │         │
   │    ┌────┴─────────┴─────────┘
   │    │  Staged TSV (shared format)
   │    │  host\ttimestamp\tlog_date\t...
   │    │
   │    ├──→ NASALogDriver.java (Hadoop Mapper/Reducer)
   │    ├──→ pig_q1.pig / pig_q2.pig / pig_q3.pig
   │    └──→ hive_setup.hql + hive_q1/q2/q3.hql
   │
   └────────────┬────────────────────
                │
   ┌────────────▼────────────────────┐
   │   Shared Core Infrastructure     │
   │                                   │
   │  LogParser.java      — regex      │
   │  LogicalBatchProcessor.java       │
   │    (fixed / monthly / weekly)     │
   │  ParsedLog.java      — POJO      │
   │  Config.java          — settings  │
   │  DatabaseManager.java — MySQL     │
   │    (load + report + correctness)  │
   └───────────────────────────────────┘
```

---

## 3. Repository Structure

```
NOSQL_PROJECT/
├── pom.xml                          # Maven build (Java 17, Hadoop 3.3.6, MongoDB, MySQL)
├── README.md
├── config.py                        # Legacy Python config (Phase 1 reference)
├── sql/
│   └── schema.sql                   # Full MySQL schema (5 tables)
├── scripts/
│   ├── download_data.sh             # Downloads NASA HTTP logs (~37MB compressed)
│   ├── run_all.sh                   # Automated end-to-end run
│   ├── pig/
│   │   ├── pig_q1.pig               # Pig Latin — Daily Traffic Summary
│   │   ├── pig_q2.pig               # Pig Latin — Top 20 Resources
│   │   └── pig_q3.pig               # Pig Latin — Hourly Error Analysis
│   └── hive/
│       ├── hive_setup.hql           # Create external table over staged TSV
│       ├── hive_q1.hql              # HiveQL — Daily Traffic Summary
│       ├── hive_q2.hql              # HiveQL — Top 20 Resources
│       └── hive_q3.hql              # HiveQL — Hourly Error Analysis
├── src/main/java/com/invincibleagam/
│   ├── Main.java                    # CLI controller (pipeline selector)
│   ├── core/
│   │   ├── Config.java              # DB hosts, ports, file paths
│   │   ├── LogParser.java           # Shared regex parser
│   │   ├── BatchProcessor.java      # Original fixed-size batch iterator
│   │   ├── BatchStrategy.java       # Enum: FIXED / MONTHLY / WEEKLY
│   │   ├── LogicalBatchProcessor.java # Month/week/fixed batch iterator
│   │   └── DatabaseManager.java     # MySQL DDL, loading, reporting, correctness check
│   ├── models/
│   │   └── ParsedLog.java           # Parsed log line POJO
│   └── pipelines/
│       ├── MongoPipeline.java       # MongoDB aggregation pipeline
│       ├── MapReducePipeline.java   # Hadoop MapReduce orchestrator
│       ├── NASALogDriver.java       # Hadoop Mapper/Reducer classes (Q1, Q2, Q3)
│       ├── PigPipeline.java         # Apache Pig orchestrator
│       └── HivePipeline.java        # Apache Hive orchestrator
└── data/
    └── raw/                         # NASA log files (downloaded by script)
```

---

## 4. Prerequisites

| Software | Version | Purpose | Install (macOS) |
|----------|---------|---------|-----------------|
| Java JDK | 17+ | Build & run | `brew install openjdk@17` |
| Maven | 3.9+ | Build | `brew install maven` |
| MySQL | 8.x | Result storage | `brew install mysql` |
| MongoDB | 7.x | MongoDB pipeline | `brew install mongodb-community` |
| Hadoop | 3.3+ | MapReduce pipeline | `brew install hadoop` |
| Apache Pig | 0.17+ | Pig pipeline | `brew install pig` |
| Apache Hive | 3.x+ | Hive pipeline | `brew install hive` |

> **Note:** MySQL runs on port **3307** and MongoDB on port **27017** by default. Edit `src/.../core/Config.java` to change.

---

## 5. Setup & Installation

```bash
# 1. Clone the repository
git clone https://github.com/InvincibleAgam/NOSQL_PROJECT.git
cd NOSQL_PROJECT

# 2. Download NASA HTTP access logs (~37MB compressed → ~370MB decompressed)
bash scripts/download_data.sh

# 3. Ensure MySQL is running (port 3307, root, no password)
#    The tool auto-creates the database and tables on first run.
mysql.server start   # or: brew services start mysql

# 4. Ensure MongoDB is running (port 27017) — only needed for MongoDB pipeline
mongod --dbpath /usr/local/var/mongodb &   # or: brew services start mongodb-community

# 5. Build the Java project (creates uber JAR with all dependencies)
mvn clean package

# 6. Verify the build
ls -lh target/nosql-project-1.0-SNAPSHOT-jar-with-dependencies.jar
```

---

## 6. Running the Project

### Interactive CLI (recommended)

```bash
java -cp target/nosql-project-1.0-SNAPSHOT-jar-with-dependencies.jar com.invincibleagam.Main
```

This launches the interactive menu:

```
================================================================================
  NASA HTTP Log Analysis Tool — NoSQL Systems Project (Java)
================================================================================

  ✓  Found: data/raw/NASA_access_log_Jul95
  ✓  Found: data/raw/NASA_access_log_Aug95

Select a pipeline:
  1. MongoDB Aggregation Pipeline
  2. Hadoop MapReduce Pipeline
  3. Apache Pig Pipeline
  4. Apache Hive Pipeline
  5. Run ALL 4 Pipelines + Correctness Check
  6. View Report for a Run

Choice [1-6]:
```

After selecting a pipeline, you'll be prompted for:
1. **Batch Strategy** — Fixed (default) / Monthly / Weekly
2. **Batch Size** — Number of records per batch (default: 10,000; only for Fixed strategy)
3. **Query Selection** — All, individual (Q1/Q2/Q3), or custom combination

### Run a single pipeline (example: MongoDB)

```bash
java -cp target/nosql-project-1.0-SNAPSHOT-jar-with-dependencies.jar com.invincibleagam.Main <<EOF
1
1
10000
a
EOF
```

### Run all 4 pipelines + correctness check

```bash
java -cp target/nosql-project-1.0-SNAPSHOT-jar-with-dependencies.jar com.invincibleagam.Main <<EOF
5
1

a
EOF
```

### Automated end-to-end run (MongoDB + MapReduce)

```bash
bash scripts/run_all.sh
```

---

## 7. Pipelines

### 7.1 MongoDB Pipeline (`MongoPipeline.java`)
- **Technology**: MongoDB Aggregation Framework (Java Driver Sync 5.0)
- **Flow**: Parse → Insert documents to MongoDB → Run `$group`, `$sort`, `$limit` aggregations → Load results to MySQL
- **Execution**: In-process via Java MongoDB driver

### 7.2 MapReduce Pipeline (`MapReducePipeline.java` + `NASALogDriver.java`)
- **Technology**: Native Hadoop MapReduce API
- **Flow**: Parse → Stage as TSV → Execute `hadoop jar` with Mapper/Reducer classes → Read output → Load to MySQL
- **Execution**: `hadoop jar target/nosql-project-1.0-SNAPSHOT-jar-with-dependencies.jar`

### 7.3 Pig Pipeline (`PigPipeline.java` + `scripts/pig/*.pig`)
- **Technology**: Apache Pig Latin (local mode)
- **Flow**: Parse → Stage as TSV → Execute `pig -x local -f` for each query script → Read output → Load to MySQL
- **Scripts**: `pig_q1.pig` (GROUP BY), `pig_q2.pig` (nested DISTINCT), `pig_q3.pig` (nested FILTER)

### 7.4 Hive Pipeline (`HivePipeline.java` + `scripts/hive/*.hql`)
- **Technology**: HiveQL (embedded metastore)
- **Flow**: Parse → Stage as TSV → Create external table (`hive_setup.hql`) → Execute `hive -f` for each query → Read output → Load to MySQL
- **Scripts**: `hive_q1.hql` (GROUP BY), `hive_q2.hql` (subquery + LIMIT), `hive_q3.hql` (CASE WHEN)

### Shared Infrastructure

All 4 pipelines share:
- **`LogParser.java`**: Rigorous regex pattern — `^(\S+)\s+\S+\s+\S+\s+\[([^\]]+)\]\s+"([^"]*)"\s+(\d{3})\s+(\S+)$`
- **`LogicalBatchProcessor.java`**: Streams log files with GZIP support; groups by fixed-size, calendar month, or ISO week
- **`DatabaseManager.java`**: MySQL DDL creation, result loading, reporting, and pairwise correctness checking

---

## 8. Mandatory Queries

| Query | Description | Output Schema |
|-------|-------------|---------------|
| **Q1** | Daily Traffic Summary | `log_date, status_code, request_count, total_bytes` |
| **Q2** | Top 20 Requested Resources | `resource_path, request_count, total_bytes, distinct_host_count` |
| **Q3** | Hourly Error Analysis | `log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts` |

Error status codes: **400–599** (inclusive).

---

## 9. Parsing Strategy

The shared `LogParser.java` uses a strict regex to extract:
`host`, `timestamp`, `log_date`, `log_hour`, `http_method`, `resource_path`, `protocol_version`, `status_code`, `bytes_transferred`

- **Bytes field**: `-` is treated as `0`
- **Malformed records**: If a line fails regex or timestamp parsing, `LogParser.parse()` returns `null`. Pipelines **never silently drop** these records — they increment a per-batch and per-run `malformed_count` that is stored in both `run_metadata` and `batch_metadata` tables.
- **Observed**: ~33 malformed records out of 3,461,613 total lines.

---

## 10. Batching Strategy

| Strategy | Batch Key | Description |
|----------|-----------|-------------|
| **Fixed** | `batch_1`, `batch_2`, ... | N records per batch (default 10,000) |
| **Monthly** | `1995-07`, `1995-08` | All records from one calendar month |
| **Weekly** | `1995-W27`, `1995-W28`, ... | All records from one ISO week |

- **Average batch size** = `total_records / num_batches`
- **Batch IDs** start from 1 and increase sequentially
- Per-batch metadata (key, strategy, valid/malformed counts, timestamps) is stored in `batch_metadata`

---

## 11. Database Schema

MySQL database: `nasa_log_analysis` (auto-created on first run)

| Table | Purpose |
|-------|---------|
| `query1_results` | Daily Traffic Summary results per pipeline run |
| `query2_results` | Top 20 Resources results per pipeline run |
| `query3_results` | Hourly Error Analysis results per pipeline run |
| `run_metadata` | Per-run summary (pipeline, batch strategy, runtime, malformed count) |
| `batch_metadata` | Per-batch details (batch key, record counts, timestamps) |

All result tables include: `pipeline_name`, `run_id`, `batch_id`, `execution_time`.

Full DDL: [`sql/schema.sql`](sql/schema.sql)

---

## 12. Correctness Check

**Option 5** in the CLI runs all 4 pipelines sequentially, then performs **6 pairwise comparisons** (MongoDB↔MR, MongoDB↔Pig, MongoDB↔Hive, MR↔Pig, MR↔Hive, Pig↔Hive).

For each pair, it compares:
- Q1: Aggregated `(log_date, status_code)` → `request_count`, `total_bytes`
- Q2: Top 20 `resource_path` → `request_count`, `total_bytes`, `distinct_host_count`
- Q3: Aggregated `(log_date, log_hour)` → `error_request_count`, `total_request_count`

Result: `✅ ALL QUERIES MATCH` or `❌ MISMATCH DETECTED` with diff details.

---

## 13. Phase History

### Phase 1 (Completed)
- ✅ System architecture designed
- ✅ Shared parser (`LogParser.java`) with regex + malformed record tracking
- ✅ Fixed-size batching via `BatchProcessor.java`
- ✅ MongoDB pipeline with aggregation framework
- ✅ MapReduce pipeline with native Hadoop Mapper/Reducer
- ✅ MySQL reporting schema (4 tables)
- ✅ CLI controller with pipeline selection
- ✅ Report generation from MySQL

### Phase 2 (Completed)
- ✅ Apache Pig pipeline with 3 Pig Latin scripts
- ✅ Apache Hive pipeline with HiveQL scripts + external table setup
- ✅ Logical batching (Monthly / Weekly) via `LogicalBatchProcessor.java`
- ✅ `batch_metadata` table for per-batch tracking
- ✅ Per-batch malformed record counters
- ✅ Query-wise selective execution (run only Q1, Q2, Q3, or any combination)
- ✅ 4-way pairwise correctness checker
- ✅ Updated CLI with all 4 pipelines + "Run All + Check" option
