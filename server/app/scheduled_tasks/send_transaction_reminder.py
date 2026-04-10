from notifications import _record_cron_run, send_notification


if __name__ == "__main__":
    resp = send_notification(
        title="Upload Transactions 💰",
        body="Don't forget to upload your transactions for this month!"
    )

    try:
        _record_cron_run(
            job="send_transaction_reminder",
            success=resp.get("message", '') == "Success!",
        )
    except Exception as tracker_err:
        print(f"[CronJobMailer] Failed to record cron run for {"send_transaction_reminder"!r}: {tracker_err}")