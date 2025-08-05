import requests
import pandas as pd
from datetime import datetime
import time
import os

# Parámetros de conexión
TEAM_ID = "9009011702"
TOKEN = "pk_75418362_0SNHEACGYFWU5R3B17EZBIN2U3U2F4ND"
HEADERS = {"Authorization": TOKEN}
BASE_URL = "https://api.clickup.com/api/v2"
START_DATE = int(datetime(2024, 1, 1).timestamp() * 1000)
END_DATE = int(datetime.now().timestamp() * 1000)

# Rutas
CSV_PATH = "DB/private.csv"
EXISTING_CSV_PATH = "DB/all_time_entries.csv"

# 🔁 Convertir timestamp a formato legible
def convert_timestamp(ms):
    if not ms:
        return None
    return datetime.fromtimestamp(int(ms) / 1000).isoformat()

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

# 📌 Guardar entradas como CSV, filtrando contra base previa
def save_entries_to_csv(entries, csv_path=CSV_PATH, existing_path=EXISTING_CSV_PATH):
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)

    # Cargar task_ids previos si existe
    existing_task_ids = set()
    if os.path.exists(existing_path):
        existing_df = pd.read_csv(existing_path)
        existing_task_ids = set(existing_df['task_id'].astype(str))

    rows = []
    for entry in entries:
        task = entry.get("task", {})
        if not isinstance(task, dict):
            task = {}

        task_id = str(task.get("id", ""))
        if task_id in existing_task_ids:
            continue  # Saltar si ya existe ese task_id

        rows.append({
            "entry_id": entry.get("id"),
            "task_id": task_id,
            "task_name": task.get("name"),
            "user_id": entry.get("user", {}).get("id"),
            "username": entry.get("user", {}).get("username"),
            "start_time": convert_timestamp(entry.get("start")),
            "stop_time": convert_timestamp(entry.get("end")),
            "duration_hours": round(int(entry.get("duration", 0)) / 3600000, 2),
            "Billable": entry.get("billable", False),
            "WorkspaceID": entry.get("workspace_id", "NA"),  # puede que no venga
            "description": entry.get("description", "NA"),
            "list_id": task.get("list", {}).get("id", "NA"),
            "folder_id": task.get("folder", {}).get("id", "NA"),
            "space_id": task.get("space", {}).get("id", "NA"),
            "task_url": entry.get("task_url"),
            "client": "NA",  # se rellena luego si tienes lógica para esto,
            "source_file": "Private"
        })

    if not rows:
        print("✅ No hay nuevas entradas con task_id nuevo para guardar.")
        return

    df = pd.DataFrame(rows)
    df.to_csv(csv_path, index=False)
    print(f"✅ CSV con {len(df)} nuevas entradas guardado en {csv_path}")

# 🧠 Pipeline principal
if __name__ == "__main__":
    print("Obteniendo usuarios...")
    users = get_assignees(TEAM_ID)
    #users = users[:10]  # Limitar a los primeros 10 usuarios para pruebas`
    print(f"→ Procesando {len(users)} usuarios")
    all_entries = []
    for i, uid in enumerate(users, 1):
        entries = get_time_entries(uid)
        all_entries.extend(entries)
        print(f"→ {i}/{len(users)}: {len(entries)} entradas (acumuladas: {len(all_entries)})")
        time.sleep(0.5)  # Por si ClickUp tiene rate limits

    print("Guardando como CSV...")
    save_entries_to_csv(all_entries)
