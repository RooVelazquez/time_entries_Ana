import os
import time

import requests
from requests.exceptions import JSONDecodeError, RequestException


BASE_URL = "https://api.clickup.com/api/v2"
REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 2


def get_clickup_token():
    token = os.getenv("CLICKUP_TOKEN") or os.getenv("TOKEN")
    if not token:
        raise RuntimeError(
            "Missing ClickUp token. Set CLICKUP_TOKEN in the environment."
        )
    return token


def get_headers():
    return {"Authorization": get_clickup_token()}


def safe_get_json(url, headers, params=None, context="", max_retries=MAX_RETRIES):
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()

            if not response.text.strip():
                raise ValueError("Empty response body")

            return response.json()
        except (JSONDecodeError, ValueError) as exc:
            print(
                f"⚠️ Invalid JSON from ClickUp for {context or url} "
                f"(attempt {attempt}/{max_retries}): {exc}"
            )
        except RequestException as exc:
            print(
                f"⚠️ Request failed for {context or url} "
                f"(attempt {attempt}/{max_retries}): {exc}"
            )

        if attempt < max_retries:
            time.sleep(RETRY_DELAY_SECONDS)

    print(f"❌ Giving up on {context or url}; using empty payload.")
    return {}


def get_team_member_ids(team_id, headers, base_url=BASE_URL):
    url = f"{base_url}/team/{team_id}"
    data = safe_get_json(url, headers, context=f"team {team_id}")
    members = data.get("team", data).get("members", []) if isinstance(data, dict) else []

    return [
        str(member["user"]["id"])
        for member in members
        if member.get("user", {}).get("role_key") in ("owner", "admin", "member")
    ]


def get_time_entries_payload(
    team_id,
    user_id,
    headers,
    start_date,
    end_date,
    base_url=BASE_URL,
    extra_params=None,
):
    url = f"{base_url}/team/{team_id}/time_entries"
    params = {
        "assignee": user_id,
        "start_date": start_date,
        "end_date": end_date,
    }
    if extra_params:
        params.update(extra_params)

    data = safe_get_json(
        url,
        headers,
        params=params,
        context=f"time entries for user {user_id}",
    )
    if not isinstance(data, dict):
        return []
    return data.get("data", [])
