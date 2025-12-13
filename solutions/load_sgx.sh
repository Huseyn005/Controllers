#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Find flag_scanner.py anywhere under Hackaton
LOAD_SGX_SCANNER="$(find "$ROOT_DIR" -type f -name "load_sgx.py" -print -quit)"

if [ -z "$LOAD_SGX_SCANNER" ]; then
  echo "ERROR: load_sgx.py not found under $ROOT_DIR" >&2
  exit 1
fi

echo "Using: $LOAD_SGX_SCANNER"

# Forward ALL arguments to Python 
python "$LOAD_SGX_SCANNER" "$@"
