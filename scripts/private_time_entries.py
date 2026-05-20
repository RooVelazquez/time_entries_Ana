import os
import time
from datetime import datetime

import pandas as pd
import pytz
from dotenv import load_dotenv

from clickup_utils import get_headers, get_team_member_ids, get_time_entries_payload


load_dotenv()

TEAM_ID = "9009011702"
HEADERS = get_headers()

toronto_tz = pytz.timezone("America/Toronto")
start_dt = toronto_tz.localize(datetime(2024, 1, 1, 0, 0, 0))
end_dt = datetime.now(toronto_tz)
START_DATE = int(start_dt.timestamp() * 1000)
END_DATE = int(end_dt.timestamp() * 1000)

CSV_PATH = "DB/private.csv"
EXISTING_CSV_PATH = "DB/all_time_entries.csv"


def convert_timestamp(ms):
    if not ms:
        return None
    dt_utc = datetime.fromtimestamp(int(ms) / 1000, tz=pytz.utc)
    return dt_utc.astimezone(toronto_tz).isoformat()


def get_assignees(team_id):
    return get_team_member_ids(team_id, HEADERS)


def get_time_entries(user_id):
    return get_time_entries_payload(
        TEAM_ID,
        user_id,
        HEADERS,
        START_DATE,
        END_DATE,
    )


def save_entries_to_csv(entries, csv_path=CSV_PATH, existing_path=EXISTING_CSV_PATH):
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)

    existing_entry_ids = set()
    if os.path.exists(existing_path):
        existing_df = pd.read_csv(existing_path)
        if "entry_id" in existing_df.columns:
            existing_entry_ids = set(existing_df["entry_id"].astype(str))

    rows = []
    for entry in entries:
        entry_id = str(entry.get("id", ""))
        if entry_id in existing_entry_ids:
            continue

        task = entry.get("task") or {}
        rows.append(
            {
                "entry_id": entry.get("id"),
                "task_id": str(task.get("id", "")),
                "task_name": task.get("name"),
                "user_id": entry.get("user", {}).get("id"),
                "username": entry.get("user", {}).get("username"),
                "start_time": convert_timestamp(entry.get("start")),
                "stop_time": convert_timestamp(entry.get("end")),
                "duration_hours": round(int(entry.get("duration", 0)) / 3600000, 2),
                "Billable": entry.get("billable", False),
                "WorkspaceID": entry.get("wid", "NA"),
                "description": entry.get("description", "NA"),
                "list_id": task.get("list", {}).get("id", "NA"),
                "folder_id": task.get("folder", {}).get("id", "NA"),
                "space_id": task.get("space", {}).get("id", "NA"),
                "task_url": entry.get("task_url"),
                "client": "NA",
                "source_file": "Private",
            }
        )

    if not rows:
        print("✅ No hay nuevas entradas con entry_id nuevo para guardar.")
        return

    df = pd.DataFrame(rows)
    df.to_csv(csv_path, index=False)
    print(f"✅ CSV con {len(df)} nuevas entradas guardado en {csv_path}")


if __name__ == "__main__":
    print("Obteniendo usuarios...")
    users = get_assignees(TEAM_ID)
    print(f"→ Procesando {len(users)} usuarios")
    all_entries = []

    for i, uid in enumerate(users, 1):
        entries = get_time_entries(uid)
        all_entries.extend(entries)
        print(f"→ {i}/{len(users)}: {len(entries)} entradas (acumuladas: {len(all_entries)})")
        time.sleep(0.5)

    print("Guardando como CSV...")
    save_entries_to_csv(all_entries)
