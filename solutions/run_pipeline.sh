#!/bin/bash
# USAGE: ./solutions/run_pipeline.sh --data-dir /path/to/data

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

export AIRFLOW_PROJ_DIR="$(pwd)/airflow"
export DATA_DIR_PATH="$DATA_DIR"

echo "Building and starting the Docker services..."
docker compose up --build -d

echo "Waiting for Airflow Webserver to be fully healthy..."
until docker compose logs airflow_webserver 2>&1 | grep -q "Running on http://0.0.0.0:8080"; do
    sleep 5
done
echo "Airflow is ready."

# 5. Trigger the DAG (The ultimate submission test)
echo "Triggering the Seismic Reconstruction DAG..."
# Use the Airflow CLI running inside the webserver container
# (You will define the DAG file in airflow/dags later)
docker compose run --rm airflow_webserver airflow dags unpause seismic_data_reconstruction
docker compose run --rm airflow_webserver airflow dags trigger seismic_data_reconstruction

echo "Pipeline triggered. Check Airflow UI at http://localhost:8080 for status."