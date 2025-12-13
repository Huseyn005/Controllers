import os
import argparse
import pandas as pd
import psycopg2

# --- DATABASE CONFIGURATION ---
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'caspian_db')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'user')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'password')

def connect_and_query(query):
    """Connects to DB, runs query, and returns DataFrame."""
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST, database=POSTGRES_DB,
            user=POSTGRES_USER, password=POSTGRES_PASSWORD
        )
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        print(f"[DB ERROR] Validation failed: {e}")
        return pd.DataFrame()

def validate_vault():
    print(f"\n{'='*50}\n{'RUNNING DATA VAULT VALIDATION':^50}\n{'='*50}")

    # TEST 1: Hub Key Uniqueness
    df_hub_survey = connect_and_query("SELECT COUNT(DISTINCT hub_survey_key) AS unique_count, COUNT(hub_survey_key) AS total_count FROM hub_survey;")
    if not df_hub_survey.empty and df_hub_survey['unique_count'][0] == df_hub_survey['total_count'][0]:
        print("[PASS] Hub Uniqueness: Hub Survey keys are unique.")
    else:
        print("[FAIL] Hub Uniqueness: Duplicate Hub Survey keys found!")

    # TEST 2: Link Integrity (Ensure all keys in the link are valid foreign keys)
    query_integrity = """
        SELECT COUNT(l.hub_well_key) 
        FROM link_survey_well l 
        LEFT JOIN hub_well h ON l.hub_well_key = h.hub_well_key
        WHERE h.hub_well_key IS NULL;
    """
    df_integrity = connect_and_query(query_integrity)
    if not df_integrity.empty and df_integrity.iloc[0, 0] == 0:
        print("[PASS] Link Integrity: No orphaned Well Keys found in Link.")
    else:
        print(f"[FAIL] Link Integrity: Found {df_integrity.iloc[0, 0]} orphaned Well Keys!")

    # TEST 3: Physics Constraint (Depth > 0)
    query_physics = "SELECT COUNT(*) FROM sat_seismic_data WHERE depth < 0;"
    df_physics = connect_and_query(query_physics)
    if not df_physics.empty and df_physics.iloc[0, 0] == 0:
        print("[PASS] Physics Constraint: All depths are non-negative.")
    else:
        print(f"[FAIL] Physics Constraint: Found {df_physics.iloc[0, 0]} records with negative depth!")
        
    print(f"\nVALIDATION COMPLETE")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--data-dir', required=True, help="Path to input data directory (required by spec)")
    args = parser.parse_args()
    
    validate_vault()