#!/bin/bash
# ============================================================
# setup_and_run.sh
# Blockchain Transaction Ledger System
# ============================================================
# Linux automation script:
#   - Creates directory structure
#   - Sets correct file permissions
#   - Installs dependencies
#   - Runs the pipeline
#   - Logs all output
# ============================================================

set -euo pipefail   # strict mode: exit on error, undefined vars

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="$PROJECT_ROOT/logs/setup.log"
PYTHON="${PYTHON:-python3}"

# ── Colours ──────────────────────────────────────────────────
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1" | tee -a "$LOG_FILE"
}
warn() {
    echo -e "${YELLOW}[WARN]${NC} $1" | tee -a "$LOG_FILE"
}
error() {
    echo -e "${RED}[ERROR]${NC} $1" | tee -a "$LOG_FILE"
    exit 1
}

# ── 1. Directory Structure ────────────────────────────────────
log "Creating project directory structure ..."
mkdir -p \
    "$PROJECT_ROOT/data/raw" \
    "$PROJECT_ROOT/data/processed" \
    "$PROJECT_ROOT/data/warehouse" \
    "$PROJECT_ROOT/logs" \
    "$PROJECT_ROOT/tests"

# ── 2. File Permissions ───────────────────────────────────────
log "Setting file permissions ..."
# Scripts: executable by owner
chmod 755 "$PROJECT_ROOT/scripts/"*.sh 2>/dev/null || true
chmod 755 "$PROJECT_ROOT/run_pipeline.py"

# Data directories: owner read/write, group read
chmod 750 "$PROJECT_ROOT/data" \
          "$PROJECT_ROOT/data/raw" \
          "$PROJECT_ROOT/data/processed" \
          "$PROJECT_ROOT/data/warehouse"

# Source files: read-only for group/other (protect code)
find "$PROJECT_ROOT/src" -name "*.py" -exec chmod 644 {} \;

# Log directory: owner full access
chmod 700 "$PROJECT_ROOT/logs"

log "Permissions set:"
ls -la "$PROJECT_ROOT/data/"

# ── 3. Python Environment Check ───────────────────────────────
log "Checking Python version ..."
$PYTHON --version || error "Python 3 not found. Install Python 3.8+"

# ── 4. Install Dependencies ───────────────────────────────────
log "Installing Python dependencies ..."
$PYTHON -m pip install --quiet --upgrade pip
$PYTHON -m pip install --quiet \
    pandas \
    numpy \
    pyarrow \
    fastparquet \
    duckdb \
    kafka-python \
    || warn "Some packages failed to install — pipeline will use fallbacks"

log "Core packages installed."

# ── 5. Validate Project Structure ────────────────────────────
log "Validating project structure ..."
REQUIRED_FILES=(
    "run_pipeline.py"
    "src/ingestion/generate_transactions.py"
    "src/ingestion/ingest_and_clean.py"
    "src/processing/process_transactions.py"
    "src/streaming/kafka_producer.py"
    "src/orchestration/blockchain_etl_dag.py"
    "sql/schema.sql"
)

for f in "${REQUIRED_FILES[@]}"; do
    if [[ -f "$PROJECT_ROOT/$f" ]]; then
        log "  ✓ $f"
    else
        error "  ✗ Missing: $f"
    fi
done

# ── 6. Run Pipeline ───────────────────────────────────────────
log "Starting pipeline ..."
cd "$PROJECT_ROOT"
$PYTHON run_pipeline.py --records 50000 --skip-spark --skip-kafka \
    2>&1 | tee -a "$LOG_FILE"

log "════════════════════════════════════════════"
log "  Setup & Pipeline Complete ✓"
log "  Log file: $LOG_FILE"
log "════════════════════════════════════════════"
