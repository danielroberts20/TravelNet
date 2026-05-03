"""
config/settings.py
~~~~~~~~~~~~~~~~~~
Environment-sourced settings for TravelNet, validated at startup.

All values are read from the .env file (or real environment variables).
Everything else — paths, currencies, metrics, aggregation maps — lives in
config/general.py as plain Python constants.

Usage:
    from config.settings import settings

    settings.smtp_host
    settings.fx_api_key
    ...
"""

from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):

    # --- Auth tokens ---
    upload_token: str = Field(alias="UPLOAD_TOKEN")
    overland_token: str = Field(alias="OVERLAND_TOKEN")
    watchdog_token: str = Field(alias="WATCHDOG_TOKEN") 

    # --- SSH ---
    compute_host: str = Field(alias="COMPUTE_HOST")
    compute_port: int = Field(alias="COMPUTE_PORT")
    compute_username: str = Field(alias="COMPUTE_USERNAME")
    compute_password: str = Field(alias="COMPUTE_PASSWORD")

    # --- Notification webhooks ---
    custom_notification_time_sensitive: str = Field(alias="CUSTOM_NOTIFICATION_TIME_SENSITIVE")
    custom_notification_not_time_sensitive: str = Field(alias="CUSTOM_NOTIFICATION_NOT_TIME_SENSITIVE")
    warning_notification: str = Field(alias="WARNING_NOTIFICATION")
    error_notification: str = Field(alias="ERROR_NOTIFICATION")
    journal_notification: str = Field(alias="JOURNAL_NOTIFICATION")
    label_known_place_notification: str = Field(alias="LABEL_KNOWN_PLACE_NOTIFICATION")

    # --- External APIs ---
    fx_api_key: str = Field(alias="FX_API_KEY")

    # --- Wake On LAN ---
    wol_host: str = Field(alias="WOL_HOST")
    wol_api_key: str = Field(alias="WOL_API_KEY")

    # --- GitHub ---
    github_public_stats_token: str = Field(alias="GITHUB_PUBLIC_STATS_TOKEN")
    github_repo: str = Field(alias="GITHUB_REPO")

    # --- Power Consumption ---
    shelly_ip: str = Field(alias="SHELLY_IP")
    watchdog_maintenance_url: str = Field(alias="WATCHDOG_MAINTENANCE_URL")

    # --- SMTP / email alerts ---
    smtp_host: str = Field(alias="ALERT_SMTP_HOST")
    smtp_port: int = Field(alias="ALERT_SMTP_PORT")          # coerced to int automatically
    smtp_username: str = Field(alias="ALERT_SMTP_USERNAME")
    email_sender: str = Field(alias="ALERT_EMAIL_SENDER")
    email_password: str = Field(alias="ALERT_EMAIL_PASSWORD")
    email_recipient: str = Field(alias="ALERT_EMAIL_RECIPIENT")

    # --- LLM / OpenAI ---
    openai_api_key: str = Field(alias="OPENAI_API_KEY")
    openai_model: str = Field(alias="OPENAI_MODEL")

    # --- Rclone / R2 ---
    age_key_path: str = Field(alias="AGE_KEY_PATH")
    rclone_remote: str = Field(alias="RCLONE_REMOTE")
    rclone_bucket: str = Field(alias="RCLONE_BUCKET")

    # --- Trevor ---
    trevor_url: str = Field(alias="TREVOR_URL", default="http://trevor:8300")
    trevor_api_key: str = Field(alias="TREVOR_API_KEY")

    model_config = {
        "env_file": Path(__file__).parent.parent.parent / ".env",  # → server/.env
        "populate_by_name": True,
        "extra": "ignore",  # silently drop any .env vars not declared in this model
    }

    @property
    def smtp_config(self) -> dict:
        """Ready-to-unpack dict for send_email() and CronJobMailer."""
        return {
            "smtp_host": self.smtp_host,
            "smtp_port": self.smtp_port,
            "username": self.smtp_username,
            "sender": self.email_sender,
            "password": self.email_password,
            "recipient": self.email_recipient,
        }



settings = Settings()