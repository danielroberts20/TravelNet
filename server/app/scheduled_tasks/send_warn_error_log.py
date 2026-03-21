from config.editable import load_overrides
load_overrides()

from config.logging import digest_handler
from config.settings import settings
from notifications import _record_cron_run
from config.editable import load_overrides

try:
    digest_handler.flush_and_send(**settings.smtp_config
    )
    _record_cron_run("send_warn_error_log", success=True)
except Exception as e:
    _record_cron_run("send_warn_error_log", success=False, detail=str(e))
    raise