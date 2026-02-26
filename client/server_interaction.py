import logging

import requests

from client.config import DATA_DIR
from config import SERVER_URL, UPLOAD_TOKEN

logger = logging.getLogger(__name__)

def download_database():
    logger.info('Downloading database')
    with requests.get(SERVER_URL / "database", headers={'Authorization': f'Bearer {UPLOAD_TOKEN}'}, stream=True) as response:
        response.raise_for_status()  # Raises error if not 200
        output_path = DATA_DIR / "database_snapshot.db"

        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

    logger.info(f"Database snapshot saved to {output_path}")
    return output_path

def get_next_job():
    response = requests.get(SERVER_URL / "jobs" / "next", headers={'Authorization': f'Bearer {UPLOAD_TOKEN}'})
    return response.json()

def start_next_job():
    response = requests.get(SERVER_URL / "jobs" / "start-next-job", headers={'Authorization': f'Bearer {UPLOAD_TOKEN}'})
    return response.json()

def submit_data_job(code_path, requirements_path, data_file_path, timeout=3600, entry_point="main"):
    files = {
        "code": open(code_path, "rb"),
        "requirements": open(requirements_path, "rb"),
        "data_file": open(data_file_path, "rb"),
    }

    data = {
        "data_mode": "inline",
        "entry_point": entry_point,
        "timeout": timeout,
    }

    response = requests.post(
        SERVER_URL / "jobs" / "submit-job",
        headers={"Authorization": F"Bearer {UPLOAD_TOKEN}"},
        files=files,
        data=data,
    )
    if response.json().get("status") == "success":
        logger.info(f"Successfully submitted data job with id {response.json()['job_id']}")
    return response

def submit_sql_job(code_path, requirements_path, sql_query, timeout=3600, entry_point="main"):
    files = {
        "code": open(code_path, "rb"),
        "requirements": open(requirements_path, "rb"),
    }

    data = {
        "data_mode": "sql",
        "sql_query": sql_query,
        "timeout": timeout,
        "entry_point": entry_point,
    }

    response = requests.post(
        SERVER_URL / "jobs" / "submit-job",
        headers={"Authorization": F"Bearer {UPLOAD_TOKEN}"},
        files=files,
        data=data,
    )
    if response.json().get("status") == "success":
        logger.info(f"Successfully submitted SQL job with id {response.json()['job_id']}")
    return response
