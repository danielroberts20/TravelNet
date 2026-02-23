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
