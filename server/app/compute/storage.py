"""
compute/storage.py
~~~~~~~~~~~~~~~~~~
Helpers for persisting compute artefacts (code, requirements, data) to disk.
"""

import shutil
import uuid

from compute.models import Compute
from config.general import COMPUTE_DIR


def store_compute(code, requirements, data_file, data_mode, sql_query, entry_point, timeout) -> Compute:
    """Save uploaded compute files to disk and return a populated Compute object.

    Creates a dedicated subdirectory under COMPUTE_DIR named after a fresh UUID,
    writes each uploaded file into it, and constructs a Compute task with the resulting
    file paths.
    """
    compute_id = str(uuid.uuid4())
    compute_dir = COMPUTE_DIR / compute_id
    compute_dir.mkdir()

    code_path         = save_compute_file(compute_id, code, "code.py")
    requirements_path = save_compute_file(compute_id, requirements, "requirements.txt")
    data_path = None
    if data_file:
        data_path = save_compute_file(compute_id, data_file, "data_input")

    item = Compute(
        id=compute_id,
        code_path=str(code_path),
        requirements_path=str(requirements_path),
        data_mode=data_mode,
        data_path=str(data_path) if data_path else None,
        sql_query=sql_query,
        entry_point=entry_point,
        timeout=timeout,
    )

    return item


def save_compute_file(id: str, file, file_path: str):
    """Copy an uploaded file stream to COMPUTE_DIR/<id>/<file_path> and return the Path."""
    path = COMPUTE_DIR / id / file_path
    with path.open("wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    return path
