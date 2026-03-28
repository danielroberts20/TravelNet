"""
scheduled_tasks/push_public_stats.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Daily cron (7am) — queries the DB, builds the public stats payload,
and pushes docs/public_stats.json to the TravelNet GitHub repo.

The committed JSON acts as a warm cache for the docs site — it falls
back to this file if the live endpoint is unreachable.

Run via:
    cd server && ./runcron.sh scheduled_tasks.push_public_stats
"""

import base64
import json
import logging
from datetime import datetime, timezone

import requests

from config.settings import settings
from notifications import CronJobMailer
from public.util import build_public_stats

logger = logging.getLogger(__name__)

GITHUB_API = "https://api.github.com"
TARGET_PATH = "docs/public_stats.json"


def get_current_sha(headers: dict) -> str | None:
    """
    Get the current blob SHA of docs/public_stats.json from GitHub.
    Required by the GitHub Contents API to update an existing file.
    Returns None if the file doesn't exist yet (first run).
    """
    url = f"{GITHUB_API}/repos/{settings.github_repo}/contents/{TARGET_PATH}"
    resp = requests.get(url, headers=headers, timeout=10)
    if resp.status_code == 200:
        return resp.json().get("sha")
    if resp.status_code == 404:
        return None
    resp.raise_for_status()


def push_stats_to_github(payload: dict) -> None:
    """
    Commit docs/public_stats.json to the repo via the GitHub Contents API.
    Creates the file on first run, updates it on subsequent runs.
    """
    headers = {
        "Authorization": f"Bearer {settings.github_public_stats_token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    content = json.dumps(payload, indent=2, ensure_ascii=False)
    encoded = base64.b64encode(content.encode()).decode()

    sha = get_current_sha(headers)

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    body = {
        "message": f"chore: update public stats [{now}]",
        "content": encoded,
        "branch": "demo-website",
    }
    if sha:
        body["sha"] = sha

    url = f"{GITHUB_API}/repos/{settings.github_repo}/contents/{TARGET_PATH}"
    resp = requests.put(url, headers=headers, json=body, timeout=15)
    resp.raise_for_status()

    action = "Updated" if sha else "Created"
    logger.info(f"{action} {TARGET_PATH} in {settings.github_repo}")


if __name__ == "__main__":
        payload = build_public_stats()
        push_stats_to_github(payload)
        logger.info("public_stats.json pushed to GitHub successfully.")