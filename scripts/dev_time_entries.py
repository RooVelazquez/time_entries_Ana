import os
import sqlite3
import time
from datetime import datetime

import pytz
from dotenv import load_dotenv

from clickup_utils import get_headers, get_team_member_ids, get_time_entries_payload


load_dotenv()

TEAM_ID = "9009011702"
SPACE_ID = "90110332645"
HEADERS = get_headers()

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


def to_local_iso(timestamp_ms):
    if not timestamp_ms:
        return None
    return (
        datetime.fromtimestamp(int(timestamp_ms) / 1000, tz=pytz.utc)
        .astimezone(toronto_tz)
        .isoformat()
    )


def save_entries_to_db(entries, db_path="DB/dev_time_entries.db"):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    if os.path.exists(db_path):
        os.remove(db_path)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS dev_time_entries (
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

    for entry in entries:
        task = entry.get("task") or {}
        user = entry.get("user") or {}
        task_location = entry.get("task_location") or {}
        cur.execute(
            """
            INSERT OR REPLACE INTO dev_time_entries
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
                task_location.get("list_id", ""),
                task_location.get("folder_id", ""),
                task_location.get("space_id", ""),
                entry.get("task_url", ""),
                "Dev",
            ),
        )

    conn.commit()
    conn.close()


def run_pipeline():
    print("Obteniendo usuarios...")
    users = get_assignees(TEAM_ID)
    print(f"Procesando {len(users)} usuarios...")
    all_entries = []

    for i, uid in enumerate(users, 1):
        entries = get_time_entries(uid)
        all_entries.extend(entries)
        print(
            f"→ {i}/{len(users)}: {len(entries)} entradas recuperadas "
            f"(acumuladas: {len(all_entries)})"
        )
        time.sleep(0.5)

    print("Guardando en base de datos...")
    save_entries_to_db(all_entries)
    print("✅ Time entries guardadas en dev_time_entries.db")


if __name__ == "__main__":
    run_pipeline()
