#!/bin/bash
# ──────────────────────────────────────────────────────────────────────────────
# Automated End-to-End Test for NOSQL Systems Phase 1
# ──────────────────────────────────────────────────────────────────────────────

set -e

echo "========================================================================"
echo "  🚀 Starting Automated NoSQL Project Run"
echo "========================================================================"

# 1. Download Data
echo -e "\n[1/3] Checking dataset availability..."
bash scripts/download_data.sh

# 2. Build the Maven Project
echo -e "\n[2/3] Building the Java Maven Project..."
mvn clean package

# 3. Run Pipelines
echo -e "\n[3/3] Executing Pipelines..."

# Run MongoDB pipeline
echo "Running MongoDB Pipeline..."
java -cp target/nosql-project-1.0-SNAPSHOT-jar-with-dependencies.jar com.invincibleagam.Main <<EOF
1

EOF

# Run MapReduce pipeline
echo "Running MapReduce Pipeline..."
java -cp target/nosql-project-1.0-SNAPSHOT-jar-with-dependencies.jar com.invincibleagam.Main <<EOF
2

EOF

echo "========================================================================"
echo "  ✅ All pipelines executed successfully!"
echo "     Check your MySQL 'nasa_log_analysis' database for the results."
echo "========================================================================"
