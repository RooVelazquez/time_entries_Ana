import requests
import sqlite3
from datetime import datetime
import time
import os  # 👈 Added to handle folder creation

# Parámetros de conexión
TEAM_ID = "9009011702"
TOKEN = "pk_75418362_0SNHEACGYFWU5R3B17EZBIN2U3U2F4ND"
#TOKEN =  os.getenv("CLICKUP_TOKEN")
HEADERS = {"Authorization": TOKEN}
BASE_URL = "https://api.clickup.com/api/v2"
START_DATE = int(datetime(2024, 1, 1).timestamp() * 1000)
END_DATE = int(datetime.now().timestamp() * 1000)

# 📌 Obtener IDs de usuarios
def get_assignees(team_id):
    url = f"{BASE_URL}/team/{team_id}"
    r = requests.get(url, headers=HEADERS)
    data = r.json()
    team = data.get("team", data)
    members = team.get("members") or team.get("memberships")
    if not members:
        raise Exception("No se encontraron miembros.")
    return [str(m['user']['id']) for m in members]

# 📌 Obtener time entries por usuario
def get_time_entries(user_id):
    url = f"{BASE_URL}/team/{TEAM_ID}/time_entries"
    params = {
        "assignee": user_id,
        "start_date": START_DATE,
        "end_date": END_DATE
    }
    r = requests.get(url, headers=HEADERS, params=params)
    return r.json().get("data", [])


# 📌 Guardar entradas en SQLite
def save_entries_to_db(entries, db_path="DB/time_entries_all.db"):
    if os.path.exists("DB/time_entries_all.db"):
        os.remove("DB/time_entries_all.db")
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS time_entries_all (
            id TEXT PRIMARY KEY,
            user_id TEXT,
            user_name TEXT,
            task_id TEXT,
            task_name TEXT,
            start_time TEXT,
            end_time TEXT,
            duration INTEGER,
            task_url TEXT
        )
    """)

    for entry in entries:
        cur.execute("""
            INSERT OR IGNORE INTO time_entries_all 
            (id, user_id, user_name, task_id, task_name, start_time, end_time, duration, task_url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            entry.get("id"),
            entry.get("user", {}).get("id"),
            entry.get("user", {}).get("username"),
            entry.get("task", {}).get("id"),
            entry.get("task", {}).get("name"),
            convert_timestamp(entry.get("start")),
            convert_timestamp(entry.get("end")),
            entry.get("duration"),
            entry.get("task_url"),
        ))

    conn.commit()
    conn.close()

# 🧠 Pipeline principal
if __name__ == "__main__":
    print("Obteniendo usuarios...")
    users = get_assignees(TEAM_ID)
    #users = users[:2]  # Limitar a 10 usuarios para pruebas

    print(f"→ Procesando {len(users)} usuarios")
    all_entries = []
    for i, uid in enumerate(users, 1):
        entries = get_time_entries(uid)
        all_entries.extend(entries)
        print(f"→ {i}/{len(users)}: {len(entries)} entradas (acumuladas: {len(all_entries)})")
        time.sleep(0.5)  # Por si ClickUp tiene rate limits

    print("Guardando en base de datos...")
    save_entries_to_db(all_entries)
    print("✅ time_entries_all.db guardado correctamente.")
