import os
import sqlite3
import time
from datetime import datetime

import pandas as pd
import pytz
from dotenv import load_dotenv

from clickup_utils import (
    BASE_URL,
    get_headers,
    get_team_member_ids,
    get_time_entries_payload,
    safe_get_json,
)


load_dotenv()

TEAM_ID = "9009011702"
LIST_ID = "901110866706"
HEADERS = get_headers()
CLIENT_CACHE_PATH = "DB/client_names.csv"

toronto_tz = pytz.timezone("America/Toronto")
start_dt = toronto_tz.localize(datetime(2024, 1, 1, 0, 0, 0))
end_dt = datetime.now(toronto_tz)
START_DATE = int(start_dt.timestamp() * 1000)
END_DATE = int(end_dt.timestamp() * 1000)

task_client_cache = {}


def get_assignees(team_id):
    return get_team_member_ids(team_id, HEADERS)


def get_time_entries(user_id):
    return get_time_entries_payload(
        TEAM_ID,
        user_id,
        HEADERS,
        START_DATE,
        END_DATE,
        extra_params={"list_id": LIST_ID},
    )


def load_client_cache():
    if not os.path.exists(CLIENT_CACHE_PATH):
        print(f"ℹ️ Client cache not found yet: {CLIENT_CACHE_PATH}")
        return {}

    print(f"📦 Loading client cache from {CLIENT_CACHE_PATH}")
    df = pd.read_csv(CLIENT_CACHE_PATH)
    if df.empty:
        print("⚠️ Client cache CSV is empty")
        return {}

    cache = dict(zip(df["task_id"].astype(str), df["client_name"]))
    print(f"✅ Loaded {len(cache)} cached client names")
    return cache


def update_client_cache(task_id, client_name):
    if not os.path.exists(CLIENT_CACHE_PATH):
        df = pd.DataFrame(columns=["task_id", "client_name"])
    else:
        df = pd.read_csv(CLIENT_CACHE_PATH)

    if task_id not in df["task_id"].astype(str).values:
        new_row = pd.DataFrame([{"task_id": task_id, "client_name": client_name}])
        df = pd.concat([df, new_row], ignore_index=True)
        df.to_csv(CLIENT_CACHE_PATH, index=False)
        print(f"📝 Cached client '{client_name}' for task {task_id}")


def get_client_from_task(task_id):
    if task_id in task_client_cache:
        print(f"♻️ Client cache hit for task {task_id}")
        return task_client_cache[task_id]

    url = f"{BASE_URL}/task/{task_id}"
    data = safe_get_json(url, HEADERS, context=f"task {task_id}")
    custom_fields = data.get("custom_fields", []) if isinstance(data, dict) else []

    for field in custom_fields:
        if field.get("name") == "Client" and field.get("type") == "drop_down":
            value = field.get("value")
            options = field.get("type_config", {}).get("options", [])
            for option in options:
                if option.get("id") == value or option.get("orderindex") == value:
                    name = option.get("name", "Unknown")
                    task_client_cache[task_id] = name
                    update_client_cache(task_id, name)
                    print(f"🔎 Resolved client '{name}' for task {task_id}")
                    return name

    task_client_cache[task_id] = "Unknown"
    update_client_cache(task_id, "Unknown")
    print(f"⚠️ Could not resolve client for task {task_id}; using Unknown")
    return "Unknown"


def to_local_iso(timestamp_ms):
    if not timestamp_ms:
        return None
    return (
        datetime.fromtimestamp(int(timestamp_ms) / 1000, tz=pytz.utc)
        .astimezone(toronto_tz)
        .isoformat()
    )


def save_entries_to_db(entries, db_path="DB/off_page_content_time_entries.db"):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    if os.path.exists(db_path):
        os.remove(db_path)

    print(f"💾 Writing {len(entries)} off-page content entries into {db_path}")
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute(
        """
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
        """
    )

    inserted = 0
    for index, entry in enumerate(entries, 1):
        task = entry.get("task") or {}
        user = entry.get("user") or {}
        task_location = entry.get("task_location") or {}
        task_id = task.get("id", "Error")

        cur.execute(
            """
            INSERT OR REPLACE INTO off_page_content_time_entries
            (entry_id, task_id, task_name, user_id, username, start_time, stop_time, duration_hours,
             Billable, WorkspaceID, description, list_id, folder_id, space_id, task_url, client)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                entry.get("id"),
                task_id,
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
                get_client_from_task(task_id),
            ),
        )
        inserted += 1
        if index % 500 == 0:
            print(f"🪵 off-page DB progress: {index}/{len(entries)} rows processed")

    conn.commit()
    conn.close()
    print(f"✅ Finished writing {inserted} rows into {db_path}")


if __name__ == "__main__":
    print("Obteniendo usuarios...")
    users = get_assignees(TEAM_ID)
    print(f"Procesando {len(users)} usuarios...")

    task_client_cache = load_client_cache()
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
    print("✅ Time entries guardadas en off_page_content_time_entries.db")
