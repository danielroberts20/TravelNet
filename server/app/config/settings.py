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

    # --- External APIs ---
    fx_api_key: str = Field(alias="FX_API_KEY")

    # --- SMTP / email alerts ---
    smtp_host: str = Field(alias="ALERT_SMTP_HOST")
    smtp_port: int = Field(alias="ALERT_SMTP_PORT")          # coerced to int automatically
    email_sender: str = Field(alias="ALERT_EMAIL_SENDER")
    email_password: str = Field(alias="ALERT_EMAIL_PASSWORD")
    email_recipient: str = Field(alias="ALERT_EMAIL_RECIPIENT")

    model_config = {
        "env_file": Path(__file__).parent.parent.parent / ".env",  # → server/.env
        "populate_by_name": True,
    }

    @property
    def smtp_cfg(self) -> dict:
        """Ready-to-unpack dict for send_email() and CronJobMailer."""
        return {
            "smtp_host": self.smtp_host,
            "smtp_port": self.smtp_port,
            "sender": self.email_sender,
            "password": self.email_password,
            "recipient": self.email_recipient,
        }


settings = Settings()