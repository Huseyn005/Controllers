#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"  

FLAG_SCANNER="$(find "$ROOT_DIR" -type f -name "flag_scanner.py" -print -quit)"

echo "Using: $FLAG_SCANNER"
python3 "$FLAG_SCANNER"
