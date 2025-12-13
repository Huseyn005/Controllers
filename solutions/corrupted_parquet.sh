#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

EXTRACTOR_SCANNER="$(find "$ROOT_DIR" -type f -name "parquet_extractor.py" -print -quit)"

if [ -z "$EXTRACTOR_SCANNER" ]; then
  echo "ERROR: parquet_extractor.py not found under $ROOT_DIR" >&2
  exit 1
fi

echo "Using: $EXTRACTOR_SCANNER"

# Forward all CLI arguments
python "$EXTRACTOR_SCANNER" "$@"
