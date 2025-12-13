#!/bin/bash
# USAGE: ./solutions/task2_ingest.sh --data-dir /data

# 1. Parse Arguments
DATA_DIR=""
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --data-dir) DATA_DIR="$2"; shift ;;
        *) echo "Unknown parameter: $1"; exit 1 ;;
    esac
    shift
done

if [ -z "$DATA_DIR" ]; then
    echo "Error: --data-dir is required"
    exit 1
fi

# 2. Run the Python Logic
# Calls the Python script in the root /app/
python3 /app/task2_build.py --data-dir "$DATA_DIR"