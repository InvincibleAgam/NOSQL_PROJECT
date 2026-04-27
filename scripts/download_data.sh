#!/bin/bash
# ──────────────────────────────────────────────────────────────────────────────
# Download NASA HTTP Access Logs (July + August 1995)
# Source: https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html
# ──────────────────────────────────────────────────────────────────────────────

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_DIR="$SCRIPT_DIR/../data/raw"
mkdir -p "$DATA_DIR"

JULY_URL="https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz"
AUG_URL="https://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz"

echo "═══════════════════════════════════════════════════════════"
echo "  📥  Downloading NASA HTTP Access Logs"
echo "═══════════════════════════════════════════════════════════"

# Download July log
if [ -f "$DATA_DIR/NASA_access_log_Jul95" ] || [ -f "$DATA_DIR/NASA_access_log_Jul95.gz" ]; then
    echo "  ✓  July log already exists"
else
    echo "  ⏳ Downloading July 1995 log..."
    curl -L -o "$DATA_DIR/NASA_access_log_Jul95.gz" "$JULY_URL"
    echo "  ✓  July log downloaded"
fi

# Download August log
if [ -f "$DATA_DIR/NASA_access_log_Aug95" ] || [ -f "$DATA_DIR/NASA_access_log_Aug95.gz" ]; then
    echo "  ✓  August log already exists"
else
    echo "  ⏳ Downloading August 1995 log..."
    curl -L -o "$DATA_DIR/NASA_access_log_Aug95.gz" "$AUG_URL"
    echo "  ✓  August log downloaded"
fi

# Decompress
echo ""
echo "  📦 Decompressing..."
cd "$DATA_DIR"

if [ -f "NASA_access_log_Jul95.gz" ] && [ ! -f "NASA_access_log_Jul95" ]; then
    gunzip -k "NASA_access_log_Jul95.gz"
    echo "  ✓  July log decompressed"
else
    echo "  ✓  July log already decompressed"
fi

if [ -f "NASA_access_log_Aug95.gz" ] && [ ! -f "NASA_access_log_Aug95" ]; then
    gunzip -k "NASA_access_log_Aug95.gz"
    echo "  ✓  August log decompressed"
else
    echo "  ✓  August log already decompressed"
fi

# Stats
echo ""
echo "  📊 File sizes:"
ls -lh "$DATA_DIR/"NASA_access_log_* 2>/dev/null | awk '{print "     " $5 "  " $9}'
echo ""

JULY_LINES=$(wc -l < "$DATA_DIR/NASA_access_log_Jul95" 2>/dev/null || echo "?")
AUG_LINES=$(wc -l < "$DATA_DIR/NASA_access_log_Aug95" 2>/dev/null || echo "?")
echo "  Lines: July=$JULY_LINES  August=$AUG_LINES"
echo ""
echo "  ✅  Dataset ready!"
