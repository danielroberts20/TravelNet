"""
scheduled_tasks/send_cron_digest.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Safety-net flush for the daily cron digest.
 
Runs at a fixed time (09:00) after all daily jobs should have completed.
If DailyCronJobMailer already flushed early (all jobs reported on time),
cron_results will be empty and this is a no-op.
 
If any jobs are missing, sends the digest anyway with a warning for each
missing job name.
 
Run via:
    cd server && ./runcron.sh scheduled_tasks.send_cron_digest
"""
 
import logging
 
from config.logging import configure_logging
from notifications import _flush_and_send
from config.settings import settings
 
logger = logging.getLogger(__name__)
 
 
def run() -> dict:
    sent = _flush_and_send(settings.smtp_config)
    return {"sent": sent}
 
 
if __name__ == "__main__":
    configure_logging()
    result = run()
    if result["sent"]:
        logger.info("Digest sent (or nothing pending)")
    else:
        logger.error("Failed to send digest")