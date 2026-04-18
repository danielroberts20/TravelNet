from prefect import serve
from prefect.schedules import Cron

from config.schedules import SCHEDULE_CONFIGS

from scheduled_tasks.get_fx import get_fx_flow
from scheduled_tasks.geocode_places import geocode_places_flow
from scheduled_tasks.cloudflare_db_backup import cloudflare_backup_db_flow
from scheduled_tasks.backfill_gbp import backfill_gbp_flow
from scheduled_tasks.send_warn_error_log import send_warn_error_log_flow
from scheduled_tasks.reset_api_usage import reset_api_usage_flow
from scheduled_tasks.get_weather import get_weather_flow
from scheduled_tasks.backfill_place import backfill_place_flow
from scheduled_tasks.push_public_stats import push_public_stats_flow
from scheduled_tasks.send_cron_digest import send_cron_digest_flow
from scheduled_tasks.weekly_location_analysis import weekly_location_analysis_flow
from scheduled_tasks.detect_country_transitions import detect_country_transitions_flow
from scheduled_tasks.detect_timezone_transitions import detect_timezone_transitions_flow
from scheduled_tasks.detect_flights import detect_flights_flow
from scheduled_tasks.check_journal_staleness import check_journal_staleness_flow
from scheduled_tasks.send_transaction_reminder import send_transaction_reminder_flow
from scheduled_tasks.get_fx_up_to_date import get_fx_up_to_date_flow
from scheduled_tasks.check_health_gaps import check_health_gaps_flow
from scheduled_tasks.backup_db import backup_db_flow
from scheduled_tasks.flag_location_noise import flag_location_noise_flow
from scheduled_tasks.retroactive_location_scan import retroactive_location_scan_flow
from scheduled_tasks.check_watchdog import check_watchdog_flow
from scheduled_tasks.poll_shelly import poll_shelly_flow


FLOW_REGISTRY = {
    "get-fx-daily":                get_fx_flow,
    "get-fx-up-to-date":           get_fx_up_to_date_flow,
    "reset-api-usage":             reset_api_usage_flow,
    "geocode-places":              geocode_places_flow,
    "backfill-place":              backfill_place_flow,
    "weekly-location-analysis":    weekly_location_analysis_flow,
    "detect-country-transitions":  detect_country_transitions_flow,
    "detect-timezone-transitions": detect_timezone_transitions_flow,
    "detect-flights":              detect_flights_flow,
    "get-weather":                 get_weather_flow,
    "backfill-gbp":                backfill_gbp_flow,
    "send-transaction-reminder":   send_transaction_reminder_flow,
    "check-health-gaps":           check_health_gaps_flow,
    "check-journal-staleness":     check_journal_staleness_flow,
    "send-warn-error-log":         send_warn_error_log_flow,
    "send-cron-digest":            send_cron_digest_flow,
    "backup-db":                   backup_db_flow,
    "backup-db-to-cloudflare":     cloudflare_backup_db_flow,
    "push-public-stats":           push_public_stats_flow,
    "identify-location-noise":        flag_location_noise_flow,
    "retroactive-location-scan":      retroactive_location_scan_flow,
    "check-watchdog":                check_watchdog_flow,
    "poll-shelly":                 poll_shelly_flow,
}


def _deployment_tags(flow) -> list[str]:
    """Auto-derive tags from the flow object. 'notifies' is added when the
    flow has notify_on_completion registered as a completion or failure hook."""
    from notifications import notify_on_completion as _notify
    tags = []
    all_hooks = list(getattr(flow, "on_completion_hooks", None) or []) + \
                list(getattr(flow, "on_failure_hooks",   None) or [])
    if _notify in all_hooks:
        tags.append("notifies")
    return tags


if __name__ == "__main__":
    deployments = [
        FLOW_REGISTRY[name].to_deployment(
            name=FLOW_REGISTRY[name].name,
            schedule=Cron(cron, timezone="Europe/London") if cron else None,
            description=desc,
            tags=_deployment_tags(FLOW_REGISTRY[name]),
        )
        for name, (cron, desc) in SCHEDULE_CONFIGS.items()
    ]
    serve(*deployments)
