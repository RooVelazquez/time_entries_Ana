import requests
import pandas as pd
from datetime import datetime
import os

# Parámetros
TEAM_ID = "9009011702"
TOKEN = "pk_75418362_0SNHEACGYFWU5R3B17EZBIN2U3U2F4ND"
HEADERS = {"Authorization": TOKEN}
BASE_URL = "https://api.clickup.com/api/v2"
CSV_PATH = "DB/tasks_table.csv"

# 📌 Obtener miembros del equipo
def get_assignees(team_id):
    url = f"{BASE_URL}/team/{team_id}"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    data = r.json()
    members = data.get("team", data).get("members", [])
    
    return [
        str(m['user']['id']) 
        for m in members 
        if m.get('user', {}).get('role_key') in ('owner', 'admin', 'member')
    ]

# 📌 Obtener tareas para un usuario
def get_tasks_for_user(user_id):
    url = f"{BASE_URL}/team/{TEAM_ID}/task"
    params = {
        "assignees[]": user_id,
        "include_closed": "true",
        "subtasks": "true",
        "team_id": TEAM_ID
    }
    r = requests.get(url, headers=HEADERS, params=params)
    return r.json().get("tasks", [])

# 📌 Guardar en CSV
def save_tasks_to_csv(tasks, csv_path=CSV_PATH):
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    rows = []

    for task in tasks:
        task_id = task.get("id")
        project = task.get("project", {})
        tasks_project_id = project.get("id")
        tasks_project_name = project.get("name")

        rows.append({
            "task_id": task_id,
            "tasks_project_id": tasks_project_id,
            "tasks_project_name": tasks_project_name
        })

    df = pd.DataFrame(rows).drop_duplicates(subset="task_id")
    df.to_csv(csv_path, index=False)
    print(f"✅ Tareas guardadas como CSV en {csv_path}")

# 🧠 Pipeline principal
if __name__ == "__main__":
    print("Obteniendo miembros...")
    users = get_assignees(TEAM_ID)
    #users = users[:10]  # Limitar a los primeros 10 usuarios para pruebas
    print(f"Obteniendo tareas para {len(set(users))} usuarios...")

    all_tasks = []
    for i, uid in enumerate(users, 1):
        tasks = get_tasks_for_user(uid)
        all_tasks.extend(tasks)
        print(f"→ Usuario {i}/{len(users)}: {len(tasks)} tareas recuperadas (acumuladas: {len(all_tasks)})")

    print(f"Total de tareas recuperadas: {len(all_tasks)}")
    print("Guardando tareas únicas por proyecto...")
    save_tasks_to_csv(all_tasks)
