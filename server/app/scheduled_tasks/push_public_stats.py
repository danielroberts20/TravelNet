"""
scheduled_tasks/push_public_stats.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Daily cron (7am) — queries the DB, builds the public stats payload,
and pushes docs/public_stats.json to the TravelNet GitHub repo.

This file is served via GitHub Pages and acts as a warm cache for the
demo website fallback when the live endpoint is unreachable.
"""
from config.editable import load_overrides
load_overrides()

import base64
import json
from datetime import datetime, timezone

import requests

from prefect import task, flow
from prefect.logging import get_run_logger

from config.settings import settings
from public.stats import build_public_stats
from notifications import record_flow_result

GITHUB_API = "https://api.github.com"
TARGET_PATH = "docs/public_stats.json"
TARGET_BRANCH = "main"


def _get_current_sha(headers: dict) -> str | None:
    url = f"{GITHUB_API}/repos/{settings.github_repo}/contents/{TARGET_PATH}"
    resp = requests.get(url, headers=headers, timeout=10, params={"ref": TARGET_BRANCH})
    if resp.status_code == 200:
        return resp.json().get("sha")
    if resp.status_code == 404:
        return None
    resp.raise_for_status()


@task
def build_stats_payload() -> dict:
    logger = get_run_logger()
    logger.info("Building public stats payload...")
    payload = build_public_stats()
    logger.info("Public stats payload built.")
    return payload


@task(retries=2, retry_delay_seconds=10)
def push_stats_to_github(payload: dict) -> str:
    logger = get_run_logger()
    headers = {
        "Authorization": f"Bearer {settings.github_public_stats_token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    content = json.dumps(payload, indent=2, ensure_ascii=False)
    encoded = base64.b64encode(content.encode()).decode()

    sha = _get_current_sha(headers)

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    body = {
        "message": f"chore: update public stats [{now}]",
        "content": encoded,
        "branch": TARGET_BRANCH,
    }
    if sha:
        body["sha"] = sha

    url = f"{GITHUB_API}/repos/{settings.github_repo}/contents/{TARGET_PATH}"
    resp = requests.put(url, headers=headers, json=body, timeout=15)
    resp.raise_for_status()

    action = "Updated" if sha else "Created"
    logger.info(f"{action} {TARGET_PATH} in {settings.github_repo}")
    return f"{action} {TARGET_PATH}"


@flow(name="Push Public Stats")
def push_public_stats_flow():
    logger = get_run_logger()
    payload = build_stats_payload()
    result = push_stats_to_github(payload)
    logger.info("public_stats.json pushed to GitHub successfully.")
    flow_result = {"result": result}
    record_flow_result(flow_result)
    return flow_result
