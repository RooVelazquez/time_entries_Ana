import os
import sqlite3
from datetime import datetime

import pandas as pd
import pytz
from dotenv import load_dotenv

from clickup_utils import get_headers, get_team_member_ids, get_time_entries_payload


load_dotenv()

TEAM_ID = "9009011702"
SPACE_ID = "90060060754"
HEADERS = get_headers()
DB_PATH = "DB/clients_time_entries.db"
TASKS_DB_PATH = "DB/tasks_table.csv"

toronto_tz = pytz.timezone("America/Toronto")
start_dt = toronto_tz.localize(datetime(2024, 1, 1, 0, 0, 0))
end_dt = datetime.now(toronto_tz)
START_DATE = int(start_dt.timestamp() * 1000)
END_DATE = int(end_dt.timestamp() * 1000)


def get_assignees(team_id):
    return get_team_member_ids(team_id, HEADERS)


def get_time_entries(user_id):
    return get_time_entries_payload(
        TEAM_ID,
        user_id,
        HEADERS,
        START_DATE,
        END_DATE,
        extra_params={"space_id": SPACE_ID},
    )


def load_task_mapping():
    if not os.path.exists(TASKS_DB_PATH):
        print(f"⚠️ Task mapping file not found: {TASKS_DB_PATH}")
        return {}

    print(f"📚 Loading task mapping from {TASKS_DB_PATH}")
    df = pd.read_csv(TASKS_DB_PATH)
    if df.empty:
        print("⚠️ Task mapping CSV is empty")
        return {}

    df["tasks_project_id"] = df["tasks_project_id"].astype(str)
    mapping = dict(zip(df["tasks_project_id"], df["tasks_project_name"]))
    print(f"✅ Loaded {len(mapping)} task mappings")
    return mapping


def to_local_iso(timestamp_ms):
    if not timestamp_ms:
        return None
    return (
        datetime.fromtimestamp(int(timestamp_ms) / 1000, tz=pytz.utc)
        .astimezone(toronto_tz)
        .isoformat()
    )


def save_clients_to_db(entries, task_mapping):
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    print(f"💾 Writing {len(entries)} client time entries into {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(
        """
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
        """
    )
    cur.execute("DELETE FROM clients")

    inserted = 0
    for index, entry in enumerate(entries, 1):
        task = entry.get("task") or {}
        user = entry.get("user") or {}
        location = entry.get("task_location") or {}
        folder_id = str(location.get("folder_id", ""))
        cur.execute(
            """
            INSERT OR REPLACE INTO clients
            (entry_id, task_id, task_name, user_id, username, start_time, stop_time, duration_hours,
             Billable, WorkspaceID, description, list_id, folder_id, space_id, task_url, client)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                entry.get("id"),
                task.get("id", "Error"),
                task.get("name", "Error"),
                user.get("id", ""),
                user.get("username", ""),
                to_local_iso(entry.get("start")),
                to_local_iso(entry.get("end")),
                int(entry.get("duration", 0)) / 1000 / 3600,
                str(entry.get("billable", False)),
                entry.get("wid", ""),
                entry.get("description", ""),
                location.get("list_id", ""),
                location.get("folder_id", ""),
                location.get("space_id", ""),
                entry.get("task_url", ""),
                task_mapping.get(folder_id, "Unknown"),
            ),
        )
        inserted += 1
        if index % 500 == 0:
            print(f"🪵 clients DB progress: {index}/{len(entries)} rows processed")

    conn.commit()
    conn.close()
    print(f"✅ Finished writing {inserted} rows into {DB_PATH}")


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
