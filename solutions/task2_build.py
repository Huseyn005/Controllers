import os
import argparse
import glob
import hashlib
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine


# --- DATABASE CONFIGURATION ---
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres_db")
POSTGRES_DB = os.getenv("POSTGRES_DB", "caspian_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")


def get_db_engine():
    """
    Create a SQLAlchemy engine for Pandas .to_sql().
    If DB is unreachable, return None and still write Parquet outputs.
    """
    try:
        uri = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:5432/{POSTGRES_DB}"
        engine = create_engine(uri)
        with engine.connect():
            print(f"[DB] Connected to PostgreSQL at {POSTGRES_HOST}")
        return engine
    except Exception as e:
        print(f"[DB WARNING] Could not connect to PostgreSQL, will continue with Parquet only: {e}")
        return None


def write_outputs(engine, df: pd.DataFrame, table_name: str, out_dir: str):
    """Write to Postgres (best-effort) and ALWAYS write Parquet."""
    if engine is not None:
        try:
            df.to_sql(table_name, engine, if_exists="replace", index=False)
            print(f"[DB LOAD] Wrote {len(df)} rows to PostgreSQL table: {table_name}")
        except Exception as e:
            print(f"[DB ERROR] Failed to write to PostgreSQL: {e}")

    parquet_path = os.path.join(out_dir, f"{table_name}.parquet")
    df.to_parquet(parquet_path, index=False)
    print(f"[FILE LOAD] Wrote {len(df)} rows to Parquet: {parquet_path}")


def md5(val) -> str:
    return hashlib.md5(str(val).encode("utf-8")).hexdigest()


def build_vault(data_dir: str):
    # output under /opt/airflow/processed_data/raw_vault in container
    processed_dir = os.path.join(os.getcwd(), "processed_data", "raw_vault")
    os.makedirs(processed_dir, exist_ok=True)

    engine = get_db_engine()

    print(f"\n{'='*50}\n{'BUILDING RAW DATA VAULT':^50}\n{'='*50}\n")

    now = datetime.now().isoformat()

    # 1) LOAD METADATA (CSVs)
    surveys_path = os.path.join(data_dir, "master_surveys.csv")
    wells_path = os.path.join(data_dir, "master_wells.csv")

    if os.path.exists(surveys_path):
        df = pd.read_csv(surveys_path)
        df["hub_survey_key"] = df["survey_type_id"].astype(str).map(md5)

        hub_survey = df[["hub_survey_key", "survey_type_id"]].drop_duplicates().copy()
        hub_survey["load_date"] = now
        hub_survey["record_source"] = "master_surveys.csv"
        write_outputs(engine, hub_survey, "hub_survey", processed_dir)

        sat_survey = df[["hub_survey_key", "survey_type", "survey_type_id"]].copy()
        sat_survey["load_date"] = now
        write_outputs(engine, sat_survey, "sat_survey_details", processed_dir)

    if os.path.exists(wells_path):
        df = pd.read_csv(wells_path)
        df["hub_well_key"] = df["well_id"].astype(str).map(md5)

        hub_well = df[["hub_well_key", "well_id"]].drop_duplicates().copy()
        hub_well["load_date"] = now
        hub_well["record_source"] = "master_wells.csv"
        write_outputs(engine, hub_well, "hub_well", processed_dir)

        sat_well = df[["hub_well_key", "well_name", "operator", "location_lat", "location_long"]].copy()
        sat_well["load_date"] = now
        write_outputs(engine, sat_well, "sat_well_details", processed_dir)

    # 2) PROCESS RECONSTRUCTED SEISMIC DATA
    search_pattern = os.path.join(data_dir, "**", "*_reconstructed.parquet")
    files = glob.glob(search_pattern, recursive=True)

    if not files:
        print("[-] ERROR: No reconstructed parquet files found.")
        if engine is not None:
            engine.dispose()
        return

    print(f"[*] Found {len(files)} seismic reconstruction files. Ingesting traces...")
    full_df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

    # Ensure required columns exist
    required = ["survey_id", "well_id", "depth", "amplitude", "quality_flag"]
    missing = [c for c in required if c not in full_df.columns]
    if missing:
        raise RuntimeError(f"Missing columns in reconstructed parquet: {missing}. Found: {list(full_df.columns)}")

    # Add required sat metadata fields (THIS fixes your KeyError)
    full_df["ingest_source"] = "sgx_reconstruction"
    full_df["ingest_timestamp"] = now

    full_df["source_hash"] = (
        full_df[["depth", "amplitude", "quality_flag"]]
        .astype(str)
        .agg("_".join, axis=1)
        .map(md5)
    )

    # Keys & link
    full_df["hub_survey_key"] = full_df["survey_id"].astype(str).map(md5)
    full_df["hub_well_key"] = full_df["well_id"].astype(str).map(md5)
    full_df["link_survey_well_key"] = (
        full_df["survey_id"].astype(str) + "_" + full_df["well_id"].astype(str)
    ).map(md5)

    # LINK
    link_sw = full_df[["link_survey_well_key", "hub_survey_key", "hub_well_key"]].drop_duplicates().copy()
    link_sw["load_date"] = now
    link_sw["record_source"] = "sgx_reconstruction"
    write_outputs(engine, link_sw, "link_survey_well", processed_dir)

    # SAT
    sat_seismic = full_df[
        ["link_survey_well_key", "depth", "amplitude", "quality_flag", "ingest_source", "source_hash", "ingest_timestamp"]
    ].copy()
    sat_seismic["load_date"] = now
    write_outputs(engine, sat_seismic, "sat_seismic_data", processed_dir)

    if engine is not None:
        engine.dispose()

    print("\nRAW VAULT INGESTION COMPLETE")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", required=True, help="Path to input data directory")
    args = parser.parse_args()
    build_vault(args.data_dir)
