import os
import argparse
import pandas as pd
import hashlib
from datetime import datetime
import glob
import psycopg2 

# --- DATABASE CONFIGURATION ---
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres_db')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'caspian_db')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'user')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'password')

def connect_db():
    """Establishes connection to PostgreSQL using environment variables."""
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        print(f"[DB] Connected to PostgreSQL at {POSTGRES_HOST}")
        return conn
    except Exception as e:
        print(f"[ERROR] Could not connect to PostgreSQL: {e}")
        return None

def write_to_postgres(conn, df, table_name, processed_dir):
    """Writes a DataFrame to both PostgreSQL and the final Parquet file."""
    if conn:
        try:
            df.to_sql(table_name, conn, if_exists='replace', index=False)
            print(f"[DB LOAD] Wrote {len(df)} rows to PostgreSQL table: {table_name}")
        except Exception as e:
            print(f"[DB ERROR] Failed to write to PostgreSQL: {e}")
            
    # CRITICAL: Always write final output to Parquet for judging
    parquet_path = os.path.join(processed_dir, f"{table_name}.parquet")
    df.to_parquet(parquet_path, index=False)
    print(f"[FILE LOAD] Wrote {len(df)} rows to Parquet: {parquet_path}")

def generate_hash(val):
    """Creates a deterministic MD5 hash key for the Data Vault structure."""
    return hashlib.md5(str(val).encode()).hexdigest()

def build_vault(data_dir):
    # OUTPUT DESTINATION 
    processed_dir = os.path.join(os.getcwd(), "processed_data", "raw_vault")
    os.makedirs(processed_dir, exist_ok=True)
    
    conn = connect_db()
    
    print(f"\n{'='*50}\n{'BUILDING RAW DATA VAULT':^50}\n{'='*50}")

    # 1. LOAD METADATA (CSVs)
    surveys_path = os.path.join(data_dir, "master_surveys.csv")
    wells_path = os.path.join(data_dir, "master_wells.csv")
    
    # --- HUB_SURVEY & SAT_SURVEY ---
    if os.path.exists(surveys_path):
        df = pd.read_csv(surveys_path)
        df['hub_survey_key'] = df['survey_type_id'].astype(str).apply(generate_hash)
        
        hub_survey = df[['hub_survey_key', 'survey_type_id']].drop_duplicates()
        hub_survey['load_date'] = datetime.now().isoformat()
        hub_survey['record_source'] = 'master_surveys.csv'
        write_to_postgres(conn, hub_survey, "hub_survey", processed_dir)
        
        sat_survey = df[['hub_survey_key', 'survey_type', 'survey_type_id']]
        sat_survey['load_date'] = datetime.now().isoformat()
        write_to_postgres(conn, sat_survey, "sat_survey_details", processed_dir)

    # --- HUB_WELL & SAT_WELL ---
    if os.path.exists(wells_path):
        df = pd.read_csv(wells_path)
        df['hub_well_key'] = df['well_id'].astype(str).apply(generate_hash)
        
        hub_well = df[['hub_well_key', 'well_id']].drop_duplicates()
        hub_well['load_date'] = datetime.now().isoformat()
        hub_well['record_source'] = 'master_wells.csv'
        write_to_postgres(conn, hub_well, "hub_well", processed_dir)
        
        sat_well = df[['hub_well_key', 'well_name', 'operator', 'location_lat', 'location_long']]
        sat_well['load_date'] = datetime.now().isoformat()
        write_to_postgres(conn, sat_well, "sat_well_details", processed_dir)


    # 2. PROCESS RECONSTRUCTED SEISMIC DATA
    search_pattern = os.path.join(data_dir, "**", "*_reconstructed.parquet")
    files = glob.glob(search_pattern, recursive=True)
    
    if not files:
        print("[-] ERROR: No reconstructed parquet files found.")
        if conn: conn.close()
        return

    print(f"\n[*] Found {len(files)} seismic reconstruction files. Ingesting traces...")
    all_traces = [pd.read_parquet(f) for f in files]
    full_df = pd.concat(all_traces, ignore_index=True)
        
    # Generate Keys and Link 
    full_df['hub_survey_key'] = full_df['survey_id'].astype(str).apply(generate_hash)
    full_df['hub_well_key'] = full_df['well_id'].astype(str).apply(generate_hash)
    full_df['link_survey_well_key'] = (full_df['survey_id'].astype(str) + "_" + full_df['well_id'].astype(str)).apply(generate_hash)
    
    # --- LINK_SURVEY_WELL ---
    link_sw = full_df[['link_survey_well_key', 'hub_survey_key', 'hub_well_key']].drop_duplicates()
    link_sw['load_date'] = datetime.now().isoformat()
    link_sw['record_source'] = 'sgx_reconstruction'
    write_to_postgres(conn, link_sw, "link_survey_well", processed_dir)
    
    # --- SAT_SEISMIC_DATA ---
    sat_seismic = full_df[['link_survey_well_key', 'depth', 'amplitude', 'quality_flag', 'ingest_source', 'source_hash', 'ingest_timestamp']]
    sat_seismic['load_date'] = datetime.now().isoformat()
    write_to_postgres(conn, sat_seismic, "sat_seismic_data", processed_dir)
    
    if conn: conn.close()
    print(f"\nRAW VAULT INGESTION COMPLETE")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--data-dir', required=True, help="Path to input data directory")
    args = parser.parse_args()
    
    build_vault(args.data_dir)
