from config.general import EMAIL_PASSWORD, EMAIL_RECIPIENT, EMAIL_SENDER, SMTP_HOST, SMTP_PORT
from config.logging import digest_handler

digest_handler.flush_and_send(
    smtp_host=SMTP_HOST,
    smtp_port=SMTP_PORT,
    sender=EMAIL_SENDER,
    password=EMAIL_PASSWORD,
    recipient=EMAIL_RECIPIENT
)