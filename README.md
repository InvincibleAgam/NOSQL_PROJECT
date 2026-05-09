# NASA HTTP Log Analysis — NoSQL Systems End-Term Project

This repository contains the complete deliverables for the **DAS 839 NoSQL Systems End-Term Project**. The system implements a unified **Java CLI tool** that supports **all 4 required execution pipelines**: MongoDB, MapReduce, Pig, and Hive.

All pipelines process two months of NASA HTTP access logs (July & August 1995, ~3.46M records) and produce mathematically identical results for the three mandatory queries.

## 1. System Architecture

```
┌─────────────────────────────────────────────────┐
│              Java CLI Controller                 │
│         (Main.java — pipeline selector)          │
└──┬────────┬────────┬────────┬───────────────────┘
   │        │        │        │
┌──▼──┐  ┌──▼──┐  ┌──▼──┐  ┌──▼──┐
│Mongo│  │ MR  │  │ Pig │  │Hive │
│Pipe │  │Pipe │  │Pipe │  │Pipe │
└──┬──┘  └──┬──┘  └──┬──┘  └──┬──┘
   │        │        │        │
   └────────┴───┬────┴────────┘
                │
   ┌────────────▼────────────┐
   │   Shared Parser Module   │
   │  (LogParser.java — regex)│
   └────────────┬────────────┘
                │
   ┌────────────▼────────────┐
   │ LogicalBatchProcessor    │
   │ (fixed / monthly / weekly)│
   └────────────┬────────────┘
                │
   ┌────────────▼────────────┐
   │   MySQL JDBC Loader +    │
   │   Reporting Module       │
   │  (DatabaseManager.java)  │
   └─────────────────────────┘
```

## 2. Pipelines

| Pipeline | Execution Technology | Core Processing |
|----------|---------------------|-----------------|
| **MongoDB** | MongoDB Aggregation Framework (`$group`, `$sort`, `$limit`) | Java MongoDB Driver Sync |
| **MapReduce** | Native Hadoop MapReduce (Mapper/Reducer classes) | `hadoop jar` CLI |
| **Pig** | Apache Pig Latin scripts (`GROUP BY`, `FOREACH`, `DISTINCT`) | `pig -x local -f` CLI |
| **Hive** | HiveQL queries (`GROUP BY`, `COUNT`, `SUM`, `CASE WHEN`) | `hive -f` CLI |

All pipelines share:
- **Parser**: `LogParser.java` (identical regex for all)
- **Batching**: `LogicalBatchProcessor.java` (fixed / monthly / weekly)
- **MySQL Loading**: `DatabaseManager.java` (JDBC)

## 3. Mandatory Queries

| Query | Description | Output Columns |
|-------|-------------|----------------|
| Q1 | Daily Traffic Summary | `log_date, status_code, request_count, total_bytes` |
| Q2 | Top 20 Requested Resources | `resource_path, request_count, total_bytes, distinct_host_count` |
| Q3 | Hourly Error Analysis | `log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts` |

## 4. Batching Strategy

Users can choose between three batching strategies at runtime:
- **Fixed**: Traditional N-records-per-batch (default 10,000)
- **Monthly**: Group by calendar month (e.g., all July 1995 records)
- **Weekly**: Group by ISO week number

Batch metadata is stored in a dedicated `batch_metadata` table.

## 5. Database Schema

Results are stored in MySQL across these tables:
- `query1_results`, `query2_results`, `query3_results` — per-query result tables
- `run_metadata` — per-run statistics (pipeline, runtime, batch info, malformed count)
- `batch_metadata` — per-batch details (key, strategy, valid/malformed counts)

## 6. Correctness Check

The tool includes a built-in **pairwise correctness checker** (option 5 in the CLI) that:
1. Runs all 4 pipelines sequentially with the same batch settings
2. Compares every query result row-by-row across all 6 pairs
3. Reports `✅ MATCH` or `❌ MISMATCH` for each pair

## 7. Running the Project

### Prerequisites
- Java 17+, Maven
- MySQL (port 3307) and MongoDB (port 27017)
- Hadoop (for MapReduce pipeline)
- Apache Pig (for Pig pipeline): `brew install pig`
- Apache Hive (for Hive pipeline): `brew install hive`

### Commands

```bash
# 1. Download NASA dataset
bash scripts/download_data.sh

# 2. Build the Maven Project
mvn clean package

# 3. Run the interactive Java CLI
java -cp target/nosql-project-1.0-SNAPSHOT-jar-with-dependencies.jar com.invincibleagam.Main
```

### CLI Flow
```
Select a pipeline:
  1. MongoDB Aggregation Pipeline
  2. Hadoop MapReduce Pipeline
  3. Apache Pig Pipeline
  4. Apache Hive Pipeline
  5. Run ALL 4 Pipelines + Correctness Check
  6. View Report for a Run
```
