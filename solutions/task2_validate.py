import os; import sys
import argparse
import pandas as pd
import psycopg2
sys.path.insert(0, "/opt/airflow")
from db_utils import get_db_engine

def connect_and_query(query, engine):
    """Connects to DB using the SQLAlchemy Engine, runs query, and returns DataFrame."""
    if not engine:
        return pd.DataFrame()
        
    try:
        # FIX 2: Use SQLAlchemy engine with pd.read_sql
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        # Note: Some validation failure might cause SQL errors if the table wasn't created
        print(f"[DB QUERY ERROR] Query failed: {e}")
        return pd.DataFrame()

def validate_vault():
    print(f"\n{'='*50}\n{'RUNNING DATA VAULT VALIDATION':^50}\n{'='*50}")

    # Initialize Engine
    engine = get_db_engine()
    if not engine:
        print("[FAIL] Cannot proceed without database connection.")
        return

    # TEST 1: Hub Key Uniqueness
    query_hub_uniqueness = "SELECT COUNT(DISTINCT hub_survey_key) AS unique_count, COUNT(hub_survey_key) AS total_count FROM hub_survey;"
    df_hub_survey = connect_and_query(query_hub_uniqueness, engine)
    
    # FIX: Check if dataframe is empty before indexing, and handle connection failure gracefully
    if df_hub_survey.empty:
         print("[FAIL] Hub Uniqueness: Query returned empty result, table likely missing or inaccessible.")
    elif df_hub_survey['unique_count'][0] == df_hub_survey['total_count'][0]:
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
    df_integrity = connect_and_query(query_integrity, engine)
    
    if df_integrity.empty:
        print("[FAIL] Link Integrity: Query returned empty result, tables likely missing or inaccessible.")
    elif df_integrity.iloc[0, 0] == 0:
        print("[PASS] Link Integrity: No orphaned Well Keys found in Link.")
    else:
        # The IndexError is fixed by the earlier checks, now the value is safe to read
        print(f"[FAIL] Link Integrity: Found {df_integrity.iloc[0, 0]} orphaned Well Keys!")

    # TEST 3: Physics Constraint (Depth > 0)
    query_physics = "SELECT COUNT(*) FROM sat_seismic_data WHERE depth < 0;"
    df_physics = connect_and_query(query_physics, engine)
    
    if df_physics.empty:
        print("[FAIL] Physics Constraint: Query returned empty result, table likely missing or inaccessible.")
    elif df_physics.iloc[0, 0] == 0:
        print("[PASS] Physics Constraint: All depths are non-negative.")
    else:
        print(f"[FAIL] Physics Constraint: Found {df_physics.iloc[0, 0]} records with negative depth!")
        
    # Dispose of the engine connection pool
    engine.dispose()
    print(f"\nVALIDATION COMPLETE")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--data-dir', required=True, help="Path to input data directory (required by spec)")
    args = parser.parse_args()
    
    validate_vault()
