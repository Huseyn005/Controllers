# 🚀 Controllers — Deep Data Engineering Pipeline
---
## Forensics → Raw Vault → ETL/Marts → Airflow → Superset
---
This repository implements a **full end-to-end data engineering platform**, designed around a real-world *hackathon-style data challenge*. The pipeline handles data recovery from corrupted files, enforces data governance using a Raw Data Vault, and orchestrates the entire process with Apache Airflow.

---

## 🔍 High-Level Architecture Flow

The project follows a standard three-layer data warehouse approach, orchestrated by Airflow.


| Stage | Goal | Output Location | Core Technology |
| :--- | :--- | :--- | :--- |
| **1. Forensics** | Data Recovery, Quality & Format Conversion. | `processed_data/` (Clean Parquet) | Custom Python/Bash Scripts |
| **2. Raw Vault** | History, Traceability, Source Truth. | `PostgreSQL` | Python/SQL Ingestion Logic |
| **3. ETL / Marts** | Analytics-Ready, KPI-focused data structures. | `PostgreSQL` | Python/SQL Transformation Logic |
| **4. Orchestration** | Automation, Scheduling, Observability. | Airflow Metadata DB | Apache Airflow |
| **5. Dashboard** | Visualization and BI. | Browser UI | Apache Superset |

---

## 📁 Repository Structure and Key Files

| File/Folder | Purpose |
| :--- | :--- |
| `dags/` | Contains the **Airflow DAG definitions** (e.g., `seismic_reckoning_pipeline.py`). |
| `solutions/` | **Executable pipeline scripts** for Forensics, Ingestion, and Marts ETL (e.g., `flag_parquet.sh`, `task2_ingest.sh`). |
| `processed_data/` | **Target folder** for cleaned and reconstructed Parquet files, ready for ingestion. |
| `db_utils.py` | PostgreSQL helper utilities. |
| `docker-compose.yml` | Defines the core Airflow + Postgres services. |
| `docker-compose-superset.yml` | Defines the optional Superset visualization stack. |

---

## ⚙️ 1. Prerequisites and Setup

### 1.1 Requirements
* Docker Engine
* `docker-compose` (v1 is supported)

```bash
docker --version
docker-compose --version
```
### 1.2 Initial Setup

```Bash
# 1. Clone the repository
git clone [https://github.com/Huseyn005/Controllers.git](https://github.com/Huseyn005/Controllers.git)
cd Controllers

# 2. Review and configure your environment variables in .env
### Essential variables include POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
nano .env
```
## 🐳 2. Start Core Platform (Airflow + PostgreSQL)We start the database first, then the Airflow services. Ensure the SSH tunnel is active for ports 8080 and 8088 on your local machine.

### 2.1 Start Services
```Bash
# Start core services (Postgres, Airflow Webserver, Airflow Scheduler)
docker-compose up -d postgres_db airflow_webserver airflow_scheduler
# Verify status (All should show 'Up')
docker-compose ps
```
### 2.2 Access Airflow UIThe webserver publishes on port 8080.Access Airflow UI: http://localhost:8080

## 🧠 3. Airflow Initialization (First Run Only)
If your Airflow containers crash (Exit 1) or you see the database initialization error, you must re-run these steps.

### 3.1 Initialize DB and Create User
```Bash
# 1. Initialize the database schema
docker-compose exec airflow_webserver airflow db init

# 2. Create the admin user
docker-compose exec airflow_webserver airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# 3. Restart services to load the new config
docker-compose restart airflow_webserver airflow_scheduler
Login credentials: admin / admin
```

## 🧪 4. FORENSICS STAGE (Data Recovery and Quality)

This stage ensures messy inputs are transformed into clean, valid, and loadable Parquet files. The reconstructed outputs go into the processed_data/ folder.

### 4.1 Flagging Corrupted ParquetThe initial step is to quickly identify all files that fail a simple read test.
```Bas
h# Recommended Bash Wrapper (based on your file structure)
bash solutions/flag_parquet.sh /absolute/path/to/data_dir

# Direct Python Execution (Example)
python solutions/flag_scanner.py --data-dir /absolute/path/to/data_dir
```

### 4.2 Repairing Corrupted Parquet Files

The repair scripts use advanced binary logic to recover files damaged by metadata errors or extra garbage bytes.Repair Logic Explained:Garbage Trimming: The script searches for the expected Parquet marker (PAR1) at the end of the file and truncates any excess junk bytes.Metadata Brute-Force: If trimming fails, it attempts to overwrite the 4-byte metadata length field (just before the final PAR1) with candidate sizes until the file loads successfully.Critical Rule: Prevent Double ReconstructionThe output file must be saved to processed_data/ and named to prevent the repair script from endlessly reprocessing its own output (e.g., *_reconstructed_reconstructed.parquet).Bash# Cleanup command if accidental duplicates are created:
rm -f processed_data/*_reconstructed_reconstructed.parquet
4.3 Converting Custom Formats (SGX/Ghost)If the pipeline involves converting proprietary formats (e.g., .sgx):Bashbash solutions/load_sgx.sh /absolute/path/to/data_dir
Expected: Clean Parquet files produced and ready for the next stage.

## 🏛️ 5. RAW VAULT STAGE (PostgreSQL Ingestion)The Raw Vault provides a governance layer by preserving history and source truth while handling incremental loads and schema drift.
### 5.1 Vault Table PatternsTable TypePurposeKeyingHUBUnique Business Keys (e.g., Well ID, Survey Name)hub_hash_key (SHA256)LINKRelationships between keys (e.g., Well-Survey relationship)link_hash_key (SHA256)SATDescriptive, time-variant attributes (e.g., seismic data)sat_hash_key (SHA256)
### 5.2 Deterministic Key GenerationHash keys are used to ensure stable joins and reproducibility. Normalization (e.g., trimming, uppercasing) is critical before hashing.Hash Key = SHA256(UPPER(TRIM(Business Key String)))
### 5.3 Load ProcessThe ingestion task reads the reconstructed files, generates hash keys, and inserts data into the appropriate HUB/LINK/SAT tables.Bash# Example Run: Orchestrated by Airflow
bash solutions/task2_ingest.sh /absolute/path/to/data_dir
### 5.4 VerificationBash# Enter the Postgres container terminal
docker exec -it postgres_db psql -U <POSTGRES_USER> -d <POSTGRES_DB>

# Verify table existence and row counts
\dt
SELECT COUNT(*) FROM hub_well;
SELECT COUNT(*) FROM sat_seismic_data;
## 📊 6. ETL STAGE — ANALYTICS MARTSThe final layer consists of analytics-ready tables built from the Raw Vault, optimized for BI tools and KPI reporting.
### 6.1 Mart DefinitionMarts are denormalized and aggregated tables (e.g., mart_well_performance, mart_survey_quality).
### 6.2 Build MartsBash# Example Run: Orchestrated by Airflow
```bash
python solutions/mart_etl.py --data-dir /absolute/path/to/data_dir
```
### 6.3 VerificationSQL-- In the Postgres terminal
\dt mart_*
SELECT * FROM mart_well_performance LIMIT 20;
## ⏱️ 7. ORCHESTRATION — APACHE AIRFLOWThe seismic_reckoning_pipeline DAG is responsible for running the entire pipeline from end-to-end.7.1 Run the DAGAccess the Airflow UI (http://localhost:8080).Enable the DAG.Trigger the run and monitor the logs per task.Tip: If you modify your DAG code, always restart the Airflow services:Bashdocker-compose restart airflow_webserver airflow_scheduler
## 📈 8. DASHBOARD — APACHE SUPERSET (Optional)8.1 Start SupersetSuperset is started using its dedicated compose file:Bashdocker-compose -f docker-compose-superset.yml up -d
Access Superset UI: http://localhost:80888.2 Connect DataIn Superset, go to Settings → Database Connections.Use the internal Docker connection string to link to your mart database:postgresql+psycopg2://<POSTGRES_USER>:<POSTGRES_PASSWORD>@postgres_db:5432/<POSTGRES_DB>Register your mart_* tables as datasets and begin building charts and dashboards.
## 🧯 9. TROUBLESHOOTING & CHEAT SHEET
ProblemSymptomSolution CommandAirflow DB not initializedAirflow containers Exit 1 (with DB init error)docker-compose exec airflow_webserver airflow db initFile Visibility ErrorDAG fails finding solutions/script.shCheck volume mappings in docker-compose.yml (- ./solutions:/opt/airflow/solutions:rw)General Crash / Exit 1
Airflow container status is Exit 1docker-compose logs --tail=200 airflow_webserverCleanup Orphan ContainersWarnings about orphaned servicesdocker-compose down --remove-orphansVerify PostgresNeed to inspect vault/mart tablesdocker exec -it postgres_db psql -U <user> -d <db>Common Commands Cheat SheetBash# Start Core Platform
docker-compose up -d postgres_db airflow_webserver airflow_scheduler

# Stop All Containers
docker-compose down

# Stop and Remove All Containers/Volumes (Aggressive Cleanup)
docker-compose down --remove-orphans --volumes

# Initialize Airflow DB (if needed)
docker-compose run --rm airflow_webserver airflow db init
docker-compose run --rm airflow_webserver airflow users create --username admin --password admin ...

# Start Superset (optional)
docker-compose -f docker-compose-superset.yml up -d
