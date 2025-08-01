import requests
import sqlite3
from datetime import datetime
import os

# Parámetros
TEAM_ID = "9009011702"
TOKEN = "pk_75418362_0SNHEACGYFWU5R3B17EZBIN2U3U2F4ND"
#TOKEN =  os.getenv("CLICKUP_TOKEN")
HEADERS = {"Authorization": TOKEN}
BASE_URL = "https://api.clickup.com/api/v2"

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

# 📌 Convertir timestamp (ms) a date
def convert_timestamp(ts):
    if ts is None:
        return None
    return datetime.fromtimestamp(int(ts) / 1000).date()

# 📌 Guardar en SQLite
def save_tasks_to_db(tasks, db_path="DB/tasks_table.db"):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)  # 👈 Asegura que DB/ existe
    if os.path.exists("DB/tasks_table.db"):
        os.remove("DB/tasks_table.db")

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Crear tabla
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tasks_table (
            task_id TEXT PRIMARY KEY,
            tasks_project_id TEXT,
            tasks_project_name TEXT
        )
    """)
    
    for task in tasks:
        task_id = task.get("id")
        project = task.get("project", {})
        tasks_project_id = project.get("id")
        tasks_project_name = project.get("name")

        cur.execute("""
            INSERT OR REPLACE INTO tasks_table (task_id, tasks_project_id, tasks_project_name)
            VALUES (?, ?, ?)
        """, (task_id, tasks_project_id, tasks_project_name))

    conn.commit()
    conn.close()

# 🧠 Pipeline principal
if __name__ == "__main__":
    print("Obteniendo miembros...")
    users = get_assignees(TEAM_ID)
    #users=users[:10]  # Limitar a los primeros 10 usuarios para evitar demasiadas solicitudes
    
    print(f"Obteniendo tareas para {len(set(users))} usuarios...")
    all_tasks = []
    for i, uid in enumerate(users, 1):
        tasks = get_tasks_for_user(uid)
        all_tasks.extend(tasks)
        print(f"→ Usuario {i}/{len(users)}: {len(tasks)} tareas recuperadas (acumuladas: {len(all_tasks)})")

    print(f"Total de tareas recuperadas: {len(all_tasks)}")
    
    print("Guardando tareas únicas por proyecto...")
    save_tasks_to_db(all_tasks)
    
    print("✅ Tareas guardadas en tasks_table.db")
