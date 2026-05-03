"""
scheduled_tasks/reset_api_usage.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Reset the monthly API call counters for all external services (exchangerate.host
and open-meteo) so quota tracking starts fresh at the beginning of each month.

Scheduled to run on the 1st of each month.
"""
from config.editable import load_overrides
load_overrides()

from prefect import task, flow
from prefect.logging import get_run_logger

from database.exchange.fx import reset_api_usage
from notifications import notify_on_completion, record_flow_result

@task
def reset_all_api_counters() -> list[dict]:
    logger = get_run_logger()
    services = ["exchangerate.host", "open-meteo"]
    results = []
    for name in services:
        result = reset_api_usage(name)
        logger.info(
            "Reset %s: old_count=%s, old_month=%s",
            result["service"], result["old_count"], result["old_month"],
        )
        results.append(result)
    return results


@flow(name="Reset API Usage", on_failure=[notify_on_completion])
def reset_api_usage_flow():
    results = reset_all_api_counters()
    result = {r["service"]: {"old_count": r["old_count"], "old_month": r["old_month"]} for r in results}
    record_flow_result(result)
    return result
