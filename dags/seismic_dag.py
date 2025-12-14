from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# Define the path to your solution scripts (inside the container)
SOLUTION_DIR = "/opt/airflow/solutions"
DATA_DIR = "/opt/airflow/processed_data"

with DAG(
    dag_id="seismic_reckoning_pipeline",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None, # Run manually for the hackathon context
    catchup=False,
    tags=["hackathon", "data_vault", "etl"],
) as dag:
    
    # Task 1: Ingests the raw data into the Data Vault (Runs task2_build.py)
    ingesting_vault = BashOperator(
        task_id="ingesting_vault",
        bash_command=f"python {SOLUTION_DIR}/task2_build.py --data-dir {DATA_DIR}",
        cwd="/opt/airflow", 
        execution_timeout=pendulum.duration(minutes=5)
    )

    # Task 2: Transforms the Raw Vault into the Aggregated Information Marts (Runs mart_etl.py)
    building_marts = BashOperator(
        task_id="building_marts",
        bash_command=f"python {SOLUTION_DIR}/mart_etl.py --data-dir {DATA_DIR}",
        cwd="/opt/airflow",
        execution_timeout=pendulum.duration(minutes=5)
    )

    # Define the dependency flow: Marts must wait for the Raw Vault
    ingesting_vault >> building_marts
