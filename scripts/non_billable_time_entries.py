import requests
import sqlite3
from datetime import datetime
import time
import os

# Parámetros
TEAM_ID = "9009011702"
SPACE_ID = "90060111272"  # Nuevo SpaceID para esta tabla
TOKEN = "pk_75418362_0SNHEACGYFWU5R3B17EZBIN2U3U2F4ND"
#TOKEN =  os.getenv("CLICKUP_TOKEN")
HEADERS = {"Authorization": TOKEN}
BASE_URL = "https://api.clickup.com/api/v2"

# Rango de fechas (1 enero 2024 → hoy)
START_DATE = int(datetime(2024, 1, 1).timestamp() * 1000)
END_DATE = int(datetime.now().timestamp() * 1000)

# 📌 Obtener miembros del equipo
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
        "end_date": END_DATE,
        "space_id": SPACE_ID
    }
    r = requests.get(url, headers=HEADERS, params=params)
    return r.json().get("data", [])

# 📌 Guardar en base de datos SQLite
def save_entries_to_db(entries, db_path="DB/non_billable_time_entries.db"):
    if os.path.exists(db_path):
        os.remove(db_path)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS non_billable_time_entries (
            entry_id TEXT PRIMARY KEY,
            task_id TEXT,
            task_name TEXT,
            user_id TEXT,
            username TEXT,
            start_time TEXT,
            stop_time TEXT,
            duration_hours REAL,
            Billable TEXT,
            WorkspaceID TEXT,
            description TEXT,
            list_id TEXT,
            folder_id TEXT,
            space_id TEXT,
            task_url TEXT,
            client TEXT
        )
    """)
    cur.execute("DELETE FROM non_billable_time_entries")

    for entry in entries:
        entry_id = entry.get("id")
        task = entry.get("task", {})
        task_id = task.get("id", "Error")
        task_name = task.get("name", "Error")
        user = entry.get("user", {})
        user_id = user.get("id", "")
        username = user.get("username", "")
        start_time = datetime.fromtimestamp(int(entry["start"]) / 1000).isoformat()
        stop_time = datetime.fromtimestamp(int(entry["end"]) / 1000).isoformat()
        duration_hours = int(entry["duration"]) / 1000 / 3600 if entry.get("duration") else 0
        Billable = str(entry.get("billable", False))  
        WorkspaceID = entry.get("wid", "")
        description = entry.get("description", "")
        task_location = entry.get("task_location", {})
        list_id = task_location.get("list_id", "")
        folder_id = task_location.get("folder_id", "")
        space_id = task_location.get("space_id", "")
        task_url = entry.get("task_url", "")
        client = "Non-Billable"

        # Insertar o reemplazar la entrada en la base de datos
        cur.execute("""
            INSERT OR REPLACE INTO non_billable_time_entries 
            (entry_id, task_id, task_name, user_id, username, start_time, stop_time, duration_hours,
             Billable, WorkspaceID, description, list_id, folder_id, space_id, task_url, client)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (entry_id, task_id, task_name, user_id, username, start_time, stop_time, duration_hours,
              Billable, WorkspaceID, description, list_id, folder_id, space_id, task_url, client))

    conn.commit()
    conn.close()

# 🧠 Pipeline
if __name__ == "__main__":
    print("Obteniendo usuarios...")
    users = get_assignees(TEAM_ID)
    #users = users[:10]  # Limitar a los primeros 10 usuarios para pruebas
    print(f"Procesando {len(users)} usuarios...")
    all_entries = []
    for i, uid in enumerate(users, 1):
        entries = get_time_entries(uid)
        all_entries.extend(entries)
        print(f"→ {i}/{len(users)}: {len(entries)} entradas recuperadas (acumuladas: {len(all_entries)})")
        time.sleep(0.5)  # Evitar rate limits

    print("Guardando en base de datos...")
    save_entries_to_db(all_entries)
    print("✅ Entradas guardadas en non_billable_time_entries.db")
