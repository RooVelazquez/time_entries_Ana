import requests
import sqlite3
from datetime import datetime
import time
import os
import pandas as pd

# Parámetros
TEAM_ID = "9009011702"
SPACE_ID = "901110866706"
TOKEN = "pk_75418362_0SNHEACGYFWU5R3B17EZBIN2U3U2F4ND"
HEADERS = {"Authorization": TOKEN}
BASE_URL = "https://api.clickup.com/api/v2"
TASKS_DB_PATH = "DB/tasks_table.csv"
CLIENT_CACHE_PATH = "DB/client_names.csv"

# 🧠 Diccionario para cachear task_id → client
task_client_cache = {}

# Rango de fechas
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
        "list_id": SPACE_ID
    }
    r = requests.get(url, headers=HEADERS, params=params)
    return r.json().get("data", [])

# 📌 Cargar mapeo de folder_id a client
def load_task_mapping():
    df = pd.read_csv(TASKS_DB_PATH)
    df['tasks_project_id'] = df['tasks_project_id'].astype(str)
    return dict(zip(df['tasks_project_id'], df['tasks_project_name']))

# 📌 Cargar CSV de cache
def load_client_cache():
    if os.path.exists(CLIENT_CACHE_PATH):
        df = pd.read_csv(CLIENT_CACHE_PATH)
        return dict(zip(df["task_id"].astype(str), df["client_name"]))
    return {}

# 📌 Actualizar CSV de cache
def update_client_cache(task_id, client_name):
    if not os.path.exists(CLIENT_CACHE_PATH):
        df = pd.DataFrame(columns=["task_id", "client_name"])
    else:
        df = pd.read_csv(CLIENT_CACHE_PATH)

    if task_id not in df["task_id"].astype(str).values:
        new_row = pd.DataFrame([{"task_id": task_id, "client_name": client_name}])
        df = pd.concat([df, new_row], ignore_index=True)
        df.to_csv(CLIENT_CACHE_PATH, index=False)

# 📌 Obtener el valor de 'Client' desde una tarea
def get_client_from_task(task_id):
    if task_id in task_client_cache:
        return task_client_cache[task_id]

    url = f"{BASE_URL}/task/{task_id}"
    r = requests.get(url, headers=HEADERS)
    if r.status_code != 200:
        print(f"⚠️ Error al obtener task {task_id}: {r.status_code}")
        task_client_cache[task_id] = "Unknown"
        update_client_cache(task_id, "Unknown")
        return "Unknown"

    data = r.json()
    custom_fields = data.get("custom_fields", [])

    for field in custom_fields:
        if field.get("name") == "Client" and field.get("type") == "drop_down":
            value = field.get("value")
            options = field.get("type_config", {}).get("options", [])
            for option in options:
                if option.get("id") == value:
                    name = option.get("name")
                    task_client_cache[task_id] = name
                    update_client_cache(task_id, name)
                    return name

    task_client_cache[task_id] = "Unknown"
    update_client_cache(task_id, "Unknown")
    return "Unknown"

# 📌 Guardar en base de datos SQLite
def save_entries_to_db(entries, db_path="DB/off_page_content_time_entries.db"):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    if os.path.exists(db_path):
        os.remove(db_path)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS off_page_content_time_entries (
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

    task_mapping = load_task_mapping()

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
        client = get_client_from_task(task_id)

        cur.execute("""
            INSERT OR REPLACE INTO off_page_content_time_entries 
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
    print(f"Procesando {len(users)} usuarios...")

    # Pre-cargar cache
    task_client_cache = load_client_cache()

    all_entries = []
    for i, uid in enumerate(users, 1):
        entries = get_time_entries(uid)
        all_entries.extend(entries)
        print(f"→ {i}/{len(users)}: {len(entries)} entradas recuperadas (acumuladas: {len(all_entries)})")
        time.sleep(0.5)

    print("Guardando en base de datos...")
    save_entries_to_db(all_entries)
    print("✅ Time entries guardadas en off_page_content_time_entries.db")
