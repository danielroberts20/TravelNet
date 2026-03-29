import io
import logging
import zipfile

from upload.transaction.constants import WISE_SOURCE_MAP
from database.transaction.ingest.wise import insert as insert_wise
from config.notifications import send_notification

logger = logging.getLogger(__name__)

async def parse_wise_upload(contents):
    """Extract and ingest all CSV files from a Wise .zip upload.

    Each CSV inside the zip corresponds to one Wise account/pot.  The filename
    is parsed to derive the source key (used to look up the friendly name in
    WISE_SOURCE_MAP).  Processing errors for individual files are logged and
    collected but do not abort the remaining files.
    """
    received = 0
    inserted = 0
    skipped = 0
    errors = []

    try:
        zf = zipfile.ZipFile(io.BytesIO(contents))
    except zipfile.BadZipFile:
        logger.error("Invalid or corrupted zip file")
        return

    csv_files = [f for f in zf.namelist() if f.endswith(".csv")]
    if not csv_files:
        logger.error("No CSV files found in zip")
    
    for filename in csv_files:
        try:
            split_filename = filename.split("_")
            source = "_".join([split_filename[1], split_filename[2]])
            if source not in WISE_SOURCE_MAP.keys():
                logger.warning(f"No friendly name found for Wise source: {source}")
            
            file_received, file_inserted, file_skipped, file_errors = insert_wise(zf, filename, source)
            received += file_received
            inserted += file_inserted
            skipped += file_skipped
            errors.extend(file_errors)

        except Exception as e:
            logger.error(f"Error while processing Wise transaction ({filename}): {str(e)}")

    send_notification(title="Wise", 
                      body=f"{received} accounts received | {inserted} inserted | {skipped} skipped",
                      time_sensitive=False)

    return received, inserted, skipped, errors