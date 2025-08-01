import sqlite3
import pandas as pd
import os

# Archivos a unir
db_files = [
    "DB/clients_time_entries.db",
    "DB/content_time_entries.db",
    "DB/dev_time_entries.db",
    "DB/non_billable_time_entries.db",
    "DB/off_page_content_time_entries.db"
]

all_entries = []

for db_file in db_files:
    conn = sqlite3.connect(db_file)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        table_name = cursor.fetchone()[0]

        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        df["source_file"] = os.path.basename(db_file)
        all_entries.append(df)
        print(f"✅ {db_file}: {len(df)} registros")

    except Exception as e:
        print(f"⚠️ Error con {db_file}: {e}")
    
    finally:
        conn.close()

# Unir todo
merged_df = pd.concat(all_entries, ignore_index=True)

# Guardar
output_db = "DB/all_time_entries.db"
conn_out = sqlite3.connect(output_db)

# 👇 Borra tabla si ya existe
conn_out.execute("DROP TABLE IF EXISTS all_time_entries")

# 👇 Crea tabla de nuevo
merged_df.to_sql("all_time_entries", conn_out, if_exists="replace", index=False)
conn_out.close()

print(f"\n📦 Merge completo: {len(merged_df)} registros guardados en {output_db}")
