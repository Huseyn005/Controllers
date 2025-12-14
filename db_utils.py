import os
from sqlalchemy import create_engine

POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres_db')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'caspian_db')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'user')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'password')

def get_db_engine():
    try:
        DB_URI = f'postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:5432/{POSTGRES_DB}'
        engine = create_engine(DB_URI)
        
        with engine.connect():
            print(f"[DB UTIL] Successfully connected to PostgreSQL at {POSTGRES_HOST}/{POSTGRES_DB}")
        
        return engine
    except Exception as e:
        print(f"[DB UTIL ERROR] Could not create database engine: {e}")
        return None
