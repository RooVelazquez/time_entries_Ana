import os

import pandas as pd
from dotenv import load_dotenv

from clickup_utils import BASE_URL, get_headers, get_team_member_ids, safe_get_json


load_dotenv()

TEAM_ID = "9009011702"
HEADERS = get_headers()
CSV_PATH = "DB/tasks_table.csv"


def get_assignees(team_id):
    return get_team_member_ids(team_id, HEADERS)


def get_tasks_for_user(user_id):
    url = f"{BASE_URL}/team/{TEAM_ID}/task"
    params = {
        "assignees[]": user_id,
        "include_closed": "true",
        "subtasks": "true",
        "team_id": TEAM_ID,
    }
    data = safe_get_json(
        url,
        HEADERS,
        params=params,
        context=f"tasks for user {user_id}",
    )
    if not isinstance(data, dict):
        return []
    return data.get("tasks", [])


def save_tasks_to_csv(tasks, csv_path=CSV_PATH):
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    rows = []

    for task in tasks:
        project = task.get("project", {})
        rows.append(
            {
                "task_id": task.get("id"),
                "tasks_project_id": project.get("id"),
                "tasks_project_name": project.get("name"),
            }
        )

    df = pd.DataFrame(rows).drop_duplicates(subset="task_id")
    df.to_csv(csv_path, index=False)
    print(f"✅ Tareas guardadas como CSV en {csv_path}")


if __name__ == "__main__":
    print("Obteniendo miembros...")
    users = get_assignees(TEAM_ID)
    print(f"Obteniendo tareas para {len(set(users))} usuarios...")

    all_tasks = []
    for i, uid in enumerate(users, 1):
        tasks = get_tasks_for_user(uid)
        all_tasks.extend(tasks)
        print(
            f"→ Usuario {i}/{len(users)}: {len(tasks)} tareas recuperadas "
            f"(acumuladas: {len(all_tasks)})"
        )

    print(f"Total de tareas recuperadas: {len(all_tasks)}")
    print("Guardando tareas únicas por proyecto...")
    save_tasks_to_csv(all_tasks)
