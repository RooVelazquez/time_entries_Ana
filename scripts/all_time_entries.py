import os
import sqlite3

import pandas as pd


DB_PATH = "DB/all_time_entries.db"
CSV_PATH = "DB/all_time_entries.csv"

db_files = [
    "DB/clients_time_entries.db",
    "DB/content_time_entries.db",
    "DB/dev_time_entries.db",
    "DB/non_billable_time_entries.db",
    "DB/off_page_content_time_entries.db",
]


def load_table_as_dataframe(db_file):
    if not os.path.exists(db_file):
        print(f"⚠️ Missing DB file: {db_file}")
        return None

    conn = sqlite3.connect(db_file)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name LIMIT 1")
        row = cursor.fetchone()
        if not row:
            print(f"⚠️ No table found in {db_file}")
            return None

        table_name = row[0]
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        df["source_file"] = os.path.basename(db_file)
        print(f"✅ {db_file}: {len(df)} registros")
        return df
    except Exception as exc:
        print(f"⚠️ Error con {db_file}: {exc}")
        return None
    finally:
        conn.close()


all_entries = []
for db_file in db_files:
    df = load_table_as_dataframe(db_file)
    if df is not None:
        all_entries.append(df)

if all_entries:
    merged_df = pd.concat(all_entries, ignore_index=True)
else:
    merged_df = pd.DataFrame(
        columns=[
            "entry_id",
            "task_id",
            "task_name",
            "user_id",
            "username",
            "start_time",
            "stop_time",
            "duration_hours",
            "Billable",
            "WorkspaceID",
            "description",
            "list_id",
            "folder_id",
            "space_id",
            "task_url",
            "client",
            "source_file",
        ]
    )
    print("⚠️ No source data found. Writing empty consolidated output.")

os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
conn_out = sqlite3.connect(DB_PATH)
merged_df.to_sql("all_time_entries", conn_out, if_exists="replace", index=False)
conn_out.close()

print(f"\n📦 Merge completo: {len(merged_df)} registros guardados en {DB_PATH}")
merged_df.to_csv(CSV_PATH, index=False)
print("✅ CSV guardado en:", CSV_PATH)
