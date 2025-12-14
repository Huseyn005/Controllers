import os; import sys
import argparse
import pandas as pd
from datetime import datetime
# Python utility imports for Delta Lake and Arrow
import pyarrow as pa
from deltalake import write_deltalake 
sys.path.insert(0, "/opt/airflow")
from db_utils import get_db_engine 

# --- OUTPUT CONFIGURATION ---
# The target directory where the final Delta Lake files will be stored
MART_VAULT_DIR = "/opt/airflow/processed_data/mart_vault"


def write_mart_table(df, table_name, engine):
    """Writes a DataFrame to PostgreSQL and the final Mart Delta Lake file for time travel."""
    os.makedirs(MART_VAULT_DIR, exist_ok=True)
    
    # 1. Write to PostgreSQL 
    if engine:
        try:
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            print(f"[DB LOAD] Wrote {len(df)} rows to PostgreSQL table: {table_name}")
        except Exception as e:
            print(f"[DB ERROR] Failed to write to PostgreSQL {table_name}: {e}")
            
    # 2. Write final output to Delta Lake format for Time Travel (BONUS)
    delta_path = os.path.join(MART_VAULT_DIR, f"{table_name}_delta") 
    
    # Convert Pandas DataFrame to Arrow Table
    arrow_table = pa.Table.from_pandas(df) 
    
    # Write to Delta Lake format, enabling versioning/time travel
    write_deltalake(
        delta_path, 
        arrow_table, 
        mode='overwrite', 
    )
    print(f"[FILE LOAD] Wrote {len(df)} rows to Delta Lake (Time Travel Enabled): {delta_path}")


def build_aggregated_marts(data_dir):
    engine = get_db_engine()
    if not engine:
        return

    print(f"\n{'='*50}\n{'BUILDING AGGREGATED ANALYTICS MARTS':^50}\n{'='*50}")

    # 1. READ REQUIRED RDV TABLES
    try:
        print("[*] Reading Raw Data Vault tables from PostgreSQL...")
        # Hubs for keys
        df_well_hub = pd.read_sql("SELECT hub_well_key, well_id FROM hub_well", engine)
        df_survey_hub = pd.read_sql("SELECT hub_survey_key, survey_type_id FROM hub_survey", engine)
        # Satellites for metrics and link
        df_seismic_sat = pd.read_sql("SELECT link_survey_well_key, depth, amplitude, quality_flag, ingest_timestamp FROM sat_seismic_data", engine)
        df_link = pd.read_sql("SELECT * FROM link_survey_well", engine)
        df_survey_sat = pd.read_sql("SELECT survey_type_id, survey_type, source_format FROM sat_survey_details", engine).drop_duplicates()
        
    except Exception as e:
        print(f"[ERROR] Failed to read required RDV tables: {e}. Ensure task2_build.py ran successfully.")
        return
        
    # --- MERGE CORE DATASETS FOR MART BUILDING ---
    
    # 1. Join Seismic Data to Link to get Hub Keys
    df_core = pd.merge(df_seismic_sat, df_link, on='link_survey_well_key', how='inner')
    
    # 2. Join Hub Keys to Well and Survey IDs/Types
    df_core = pd.merge(df_core, df_well_hub, on='hub_well_key', how='inner')
    df_core = pd.merge(df_core, df_survey_hub, on='hub_survey_key', how='inner')
    
    # 3. Join Survey Type ID to get source format
    df_core = pd.merge(df_core, df_survey_sat, on='survey_type_id', how='left')


    # --- 2. BUILD mart_well_performance (Well Performance Summary) ---
    print("\n[*] Building mart_well_performance...")
    
    mart_well = df_core.groupby(['well_id', 'source_format']).agg(
        total_readings=('depth', 'count'),
        avg_amplitude=('amplitude', 'mean'),
        total_quality_flags=('quality_flag', lambda x: (x == 1).sum()) # Count where flag is 1
    ).reset_index()

    # Calculate Data Quality Rate (DQR)
    mart_well['data_quality_rate'] = mart_well['total_quality_flags'] / mart_well['total_readings']
    mart_well = mart_well.drop(columns=['total_quality_flags']) 
    
    write_mart_table(mart_well, "mart_well_performance", engine)


    # --- 3. BUILD mart_sensor_analysis (Sensor Reliability Summary) ---
    print("\n[*] Building mart_sensor_analysis...")
    
    # 'survey_type' acts as a proxy for 'sensor type' for aggregation
    mart_sensor = df_core.groupby('survey_type').agg(
        total_readings=('depth', 'count'),
        avg_amplitude=('amplitude', 'mean'),
        total_quality_flags=('quality_flag', lambda x: (x == 1).sum())
    ).reset_index()

    mart_sensor['data_quality_rate'] = mart_sensor['total_quality_flags'] / mart_sensor['total_readings']
    mart_sensor = mart_sensor.drop(columns=['total_quality_flags'])

    write_mart_table(mart_sensor, "mart_sensor_analysis", engine)


    # --- 4. BUILD mart_survey_summary (Data Acquisition Summary) ---
    print("\n[*] Building mart_survey_summary...")
    
    # Calculate wells surveyed per survey type (must use link table before group by)
    df_link_wells = df_link[['hub_survey_key', 'hub_well_key']].drop_duplicates()
    df_link_wells = pd.merge(df_link_wells, df_survey_hub, on='hub_survey_key', how='inner')
    wells_surveyed = df_link_wells.groupby('survey_type_id')['hub_well_key'].nunique().reset_index(name='wells_surveyed_count')
    
    # Calculate metrics grouped by survey type and source format
    mart_survey_metrics = df_core.groupby(['survey_type', 'source_format', 'survey_type_id']).agg(
        total_readings=('depth', 'count'),
        avg_amplitude=('amplitude', 'mean'),
        earliest_timestamp=('ingest_timestamp', 'min'),
        latest_timestamp=('ingest_timestamp', 'max')
    ).reset_index()

    # Final merge to include well count
    mart_survey_summary = pd.merge(mart_survey_metrics, wells_surveyed, on='survey_type_id', how='left')
    
    mart_survey_summary = mart_survey_summary[[
        'survey_type', 'source_format', 'wells_surveyed_count', 'total_readings', 
        'avg_amplitude', 'earliest_timestamp', 'latest_timestamp'
    ]]
    
    write_mart_table(mart_survey_summary, "mart_survey_summary", engine)
    
    print(f"\nINFORMATION MART BUILD COMPLETE: 3 Aggregation tables created with Delta Lake time travel enabled.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--data-dir', required=True, help="Path to input data directory")
    args = parser.parse_args()
    
    build_aggregated_marts(args.data_dir)
