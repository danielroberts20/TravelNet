from config.editable import load_overrides
load_overrides()

from prefect import task, flow
from prefect.logging import get_run_logger

from config.logging import digest_handler
from config.settings import settings
from notifications import record_flow_result, notify_on_completion, log_on_success


@task
def flush_and_send_digest():
    logger = get_run_logger()
    logger.info("Flushing DailyDigestHandler and sending warning/error email...")
    digest_handler.flush_and_send(**settings.smtp_config)
    logger.info("Digest sent successfully.")


@flow(name="Send Warn+Error Log", on_failure=[notify_on_completion], on_completion=[log_on_success])
def send_warn_error_log_flow():
    flush_and_send_digest()
    result = {"status": "completed"}
    record_flow_result(result)
    return result
