import json
import pandas as pd
from supabase import create_client

# ── Config ──────────────────────────────────────────────────────────────
SUPABASE_URL = 'https://nfnlzrmdwqmcryqjasat.supabase.co'
SUPABASE_KEY = 'sb_secret_3H6UKumBw8S3Dyhi7sdZ-g_U_s8Y-xC'

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── Files → Tables mapping ───────────────────────────────────────────────
files_and_tables = [
    ("/opt/airflow/database/Data_files/hotels.csv",      "dim_hotels"),
    ("/opt/airflow/database/Data_files/room_hotels.csv", "dim_room_hotel"),
    ("/opt/airflow/database/Data_files/restaurants.csv", "dim_restaurants"),
    ("/opt/airflow/database/Data_files/places.csv",      "dim_places"),
]

# ── Load each file ───────────────────────────────────────────────────────
for file_path, table_name in files_and_tables:
    print(f"Loading {table_name}...")
    
    df = pd.read_csv(file_path)
    data = json.loads(df.to_json(orient="records"))
    
    # upsert: inserts new rows, replaces existing ones (matched by primary key)
    supabase.table(table_name).upsert(data).execute()
    print(f"  ✅ {table_name} — {len(df)} rows loaded")

print("\n🎉 All 4 tables loaded successfully!")