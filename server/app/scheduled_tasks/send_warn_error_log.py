from config.logging import digest_handler
from config.settings import settings

digest_handler.flush_and_send(
    smtp_host=settings.smtp_host,
    smtp_port=settings.smtp_port,
    sender=settings.email_sender,
    password=settings.email_password,
    recipient=settings.email_recipient
)