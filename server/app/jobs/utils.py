import shutil
import uuid

from jobs.models import Job
from config.general import JOBS_DIR


def store_job(job_id, code, requirements, data_file, data_mode, sql_query, entry_point, timeout):
    # -------- Create Job Folder --------

    job_id = str(uuid.uuid4())
    job_dir = JOBS_DIR / job_id
    job_dir.mkdir()

    # -------- Save Files --------

    code_path = save_job_file(job_id, code, "code.py")
    requirements_path = save_job_file(job_id, requirements, "requirements.txt")
    data_path = None
    if data_file:
        data_path = save_job_file(job_id, data_file, "data_input")
    
    # -------- Create Job Object --------

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


def save_job_file(id, file, file_path):
    path = JOBS_DIR / id / file_path
    with path.open("wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    return path