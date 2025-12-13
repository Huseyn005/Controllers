#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Find flag_scanner.py anywhere under Hackaton (parent)
FLAG_SCANNER="$(find "$ROOT_DIR" -type f -name "flag_scanner.py" -print -quit)"

if [ -z "$FLAG_SCANNER" ]; then
  echo "ERROR: flag_scanner.py not found under $ROOT_DIR" >&2
  exit 1
fi

echo "Using: $FLAG_SCANNER"

# Forward ALL arguments to Python script
python3 "$FLAG_SCANNER" "$@"
