"""
jobs/utils.py
~~~~~~~~~~~~~
Helpers for persisting job artefacts (code, requirements, data) to disk.
"""

import shutil
import uuid

from jobs.models import Job
from config.general import JOBS_DIR


def store_job(job_id, code, requirements, data_file, data_mode, sql_query, entry_point, timeout) -> Job:
    """Save uploaded job files to disk and return a populated Job object.

    Creates a dedicated subdirectory under JOBS_DIR named after the job UUID,
    writes each uploaded file into it, and constructs a Job with the resulting
    file paths.

    Note: job_id parameter is ignored — a fresh UUID is always generated to
    prevent caller-supplied IDs from clashing.
    """
    # Always generate a new UUID regardless of the caller-supplied job_id
    job_id = str(uuid.uuid4())
    job_dir = JOBS_DIR / job_id
    job_dir.mkdir()

    code_path         = save_job_file(job_id, code, "code.py")
    requirements_path = save_job_file(job_id, requirements, "requirements.txt")
    data_path = None
    if data_file:
        data_path = save_job_file(job_id, data_file, "data_input")

    job = Job(
        id=job_id,
        code_path=str(code_path),
        requirements_path=str(requirements_path),
        data_mode=data_mode,
        data_path=str(data_path) if data_path else None,
        sql_query=sql_query,
        entry_point=entry_point,
        timeout=timeout,
    )

    return job


def save_job_file(id: str, file, file_path: str):
    """Copy an uploaded file stream to JOBS_DIR/<id>/<file_path> and return the Path."""
    path = JOBS_DIR / id / file_path
    with path.open("wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    return path
