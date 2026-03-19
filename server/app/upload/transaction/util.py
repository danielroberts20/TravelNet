from datetime import datetime
import io
import logging
import zipfile

from config.general import REVOLUT_TRANSACTION_BACKUP_DIR, WISE_SOURCE_MAP
from database.transaction.ingest.revolut import insert as insert_revolut
from database.transaction.ingest.wise import insert as insert_wise

logger = logging.getLogger(__name__)

async def parse_wise_upload(zip_file):
    contents = await zip_file.read()
    results = []
    errors = []

    try:
        with zipfile.ZipFile(io.BytesIO(contents)) as zf:
            csv_files = [f for f in zf.namelist() if f.endswith(".csv")]
            if not csv_files:
                logger.error("No CSV files found in zip")
            
            for filename in csv_files:
                try:
                    split_filename = filename.split("_")
                    source = "_".join([split_filename[1], split_filename[2]])
                    if source not in WISE_SOURCE_MAP.keys():
                        logger.warning(f"No friendly name found for Wise source: {source}")
                    
                    file_results, file_errors = insert_wise(zf, filename, source)
                    results.extend(file_results)
                    errors.extend(file_errors)
                except Exception as e:
                    logger.error(f"Error while processing Wise transaction ({filename}): {str(e)}")

    except zipfile.BadZipFile as e:
        logger.error("Invalid or corrupted zip file")

async def parse_revolut_upload(csv_file):
    contents = await csv_file.read()

    # Decode bytes → string
    decoded = contents.decode("utf-8")
    now = datetime.now()
    year_month = now.strftime("%Y-%m")
    day = int(now.strftime("%d"))-1
    csv_path = REVOLUT_TRANSACTION_BACKUP_DIR / f"{year_month}-{day}.csv"
    with open(csv_path, "w+") as f:
        f.write(decoded)
        f.close()

    inserted, skipped, errors = insert_revolut(csv_path)