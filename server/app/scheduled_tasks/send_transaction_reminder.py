from config.editable import load_overrides
load_overrides()

from prefect import task, flow
from prefect.logging import get_run_logger

from notifications import send_notification, record_flow_result, notify_on_completion, log_on_success


@task
def send_transaction_reminder_task() -> dict:
    logger = get_run_logger()
    logger.info("Sending transaction upload reminder notification...")
    resp = send_notification(
        title="Upload Transactions 💰",
        body="Don't forget to upload your transactions for this month!"
    )
    logger.info("Notification response: %s", resp)
    return resp


@flow(name="Send Transaction Reminder", on_failure=[notify_on_completion], on_completion=[log_on_success])
def send_transaction_reminder_flow():
    result = send_transaction_reminder_task()
    record_flow_result(result)
    return result
