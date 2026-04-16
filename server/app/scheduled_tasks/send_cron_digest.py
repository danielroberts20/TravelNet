"""
scheduled_tasks/send_cron_digest.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Safety-net flush for the daily cron digest.

Runs at a fixed time (09:00) after all daily jobs should have completed.
If DailyCronJobMailer already flushed early (all jobs reported on time),
cron_results will be empty and this is a no-op.

If any jobs are missing, sends the digest anyway with a warning for each
missing job name.
"""
from config.editable import load_overrides
load_overrides()

from prefect import task, flow
from prefect.logging import get_run_logger

from notifications import _flush_and_send, record_flow_result
from config.settings import settings


@task
def flush_daily_digest() -> dict:
    logger = get_run_logger()
    sent = _flush_and_send(settings.smtp_config)
    if sent:
        logger.info("Digest sent (or nothing pending)")
    else:
        logger.error("Failed to send digest")
    return {"sent": sent}


@flow(name="Send Digest")
def send_cron_digest_flow():
    result = flush_daily_digest()
    record_flow_result(result)
    return result
