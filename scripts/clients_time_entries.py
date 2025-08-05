import requests
import sqlite3
from datetime import datetime
import time
import os

TEAM_ID = "9009011702"
SPACE_ID = "90060060754"
TOKEN = "pk_75418362_0SNHEACGYFWU5R3B17EZBIN2U3U2F4ND"
#TOKEN =  os.getenv("CLICKUP_TOKEN")
HEADERS = {"Authorization": TOKEN}
BASE_URL = "https://api.clickup.com/api/v2"
DB_PATH = "DB/clients_time_entries.db"
TASKS_DB_PATH = "DB/tasks_table.db"

START_DATE = int(datetime(2024, 1, 1).timestamp() * 1000)
END_DATE = int(datetime.now().timestamp() * 1000)

def get_assignees(team_id):
    url = f"{BASE_URL}/team/{team_id}"
    r = requests.get(url, headers=HEADERS)
    data = r.json()
    members = data.get("team", data).get("members", [])
    return [str(m['user']['id']) for m in members]

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

def load_task_mapping():
    conn = sqlite3.connect(TASKS_DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT task_id, tasks_project_id, tasks_project_name FROM tasks_table")
    mapping = {task_id: project_name for task_id, _, project_name in cur.fetchall()}
    conn.close()
    return mapping

def save_clients_to_db(entries, task_mapping):
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)  # 👈 Asegura que DB/ existe
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS clients (
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
    cur.execute("DELETE FROM clients")

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
        billable = str(entry.get("billable", False))
        wid = entry.get("wid", "")
        description = entry.get("description", "")
        location = entry.get("task_location", {})
        list_id = location.get("list_id", "")
        folder_id = location.get("folder_id", "")
        space_id = location.get("space_id", "")
        task_url = entry.get("task_url", "")
        client = task_mapping.get(task_id, "Unknown")

        cur.execute("""
            INSERT OR REPLACE INTO clients 
            (entry_id, task_id, task_name, user_id, username, start_time, stop_time, duration_hours,
             Billable, WorkspaceID, description, list_id, folder_id, space_id, task_url, client)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (entry_id, task_id, task_name, user_id, username, start_time, stop_time, duration_hours,
              billable, wid, description, list_id, folder_id, space_id, task_url, client))

    conn.commit()
    conn.close()

if __name__ == "__main__":
    print("🔍 Obteniendo usuarios...")
    users = get_assignees(TEAM_ID)

    print("🕒 Descargando time entries...")
    all_entries = []
    for i, user_id in enumerate(users, 1):
        entries = get_time_entries(user_id)
        all_entries.extend(entries)
        print(f"→ {i}/{len(users)}: {len(entries)} entradas")

    print("📚 Cargando mapeo de tareas...")
    task_mapping = load_task_mapping()

    print("💾 Guardando en DB...")
    save_clients_to_db(all_entries, task_mapping)

    print("✅ Clientes guardados en DB/clients_time_entries.db")