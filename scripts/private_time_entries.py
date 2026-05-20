import os
import time
from datetime import datetime

import pandas as pd
import pytz
from pandas.errors import EmptyDataError
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


def get_nested_id(container, key):
    value = container.get(key)
    if isinstance(value, dict):
        return value.get("id", "NA")
    return "NA"


def save_entries_to_csv(entries, csv_path=CSV_PATH, existing_path=EXISTING_CSV_PATH):
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    print(f"💾 Preparing private CSV export into {csv_path}")
    print(f"📦 Incoming private candidate entries: {len(entries)}")

    existing_entry_ids = set()
    if os.path.exists(existing_path):
        try:
            print(f"📚 Reading existing consolidated CSV from {existing_path}")
            existing_df = pd.read_csv(existing_path)
            if "entry_id" in existing_df.columns:
                existing_entry_ids = set(existing_df["entry_id"].astype(str))
                print(f"✅ Loaded {len(existing_entry_ids)} existing entry IDs")
            else:
                print("⚠️ Existing consolidated CSV has no 'entry_id' column")
        except EmptyDataError:
            print(f"⚠️ Existing CSV is empty: {existing_path}")
        except Exception as exc:
            print(f"⚠️ Could not read existing CSV {existing_path}: {exc}")
    else:
        print(f"ℹ️ Existing consolidated CSV not found: {existing_path}")

    rows = []
    skipped_existing = 0
    for index, entry in enumerate(entries, 1):
        entry_id = str(entry.get("id", ""))
        if entry_id in existing_entry_ids:
            skipped_existing += 1
            continue

        task = entry.get("task") or {}
        user = entry.get("user") or {}
        rows.append(
            {
                "entry_id": entry.get("id"),
                "task_id": str(task.get("id", "")),
                "task_name": task.get("name"),
                "user_id": user.get("id"),
                "username": user.get("username"),
                "start_time": convert_timestamp(entry.get("start")),
                "stop_time": convert_timestamp(entry.get("end")),
                "duration_hours": round(int(entry.get("duration", 0)) / 3600000, 2),
                "Billable": entry.get("billable", False),
                "WorkspaceID": entry.get("wid", "NA"),
                "description": entry.get("description", "NA"),
                "list_id": get_nested_id(task, "list"),
                "folder_id": get_nested_id(task, "folder"),
                "space_id": get_nested_id(task, "space"),
                "task_url": entry.get("task_url"),
                "client": "NA",
                "source_file": "Private",
            }
        )
        if index % 500 == 0:
            print(
                f"🪵 private CSV progress: {index}/{len(entries)} checked, "
                f"{len(rows)} pending export, {skipped_existing} skipped"
            )

    if not rows:
        print("✅ No hay nuevas entradas con entry_id nuevo para guardar.")
        return

    print(
        f"🧾 Writing {len(rows)} new private rows "
        f"({skipped_existing} duplicates skipped)"
    )
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
