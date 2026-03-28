from collections import defaultdict
from datetime import datetime
import json
import logging
import statistics
from typing import Any

from config.general import INTERVAL_MINUTES
from upload.health.constants import METRIC_AGGREGATION, SNAKE_TO_DISPLAY
from database.health.table import insert_health_entry

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Timestamp helpers
# ---------------------------------------------------------------------------

def parse_unix(date_str: str) -> int:
    """Parse a Health Auto Export date string to a Unix timestamp.

    Handles both full datetime strings ("2024-02-06 14:30:00 -0800")
    and date-only strings ("2024-02-06") used by aggregated sleep.
    """
    date_str = date_str.strip()
    if len(date_str) == 10:
        # Date-only — treat as midnight UTC
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return int(dt.timestamp())
    dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S %z")
    return int(dt.timestamp())


def bucket_timestamp(unix_ts: int, interval_minutes: int) -> int:
    """Snap a Unix timestamp down to the start of its INTERVAL_MINUTES bucket."""
    interval_seconds = interval_minutes * 60
    return (unix_ts // interval_seconds) * interval_seconds


def _parse_sources(point: dict) -> list[str]:
    """Extract sources from a data point's 'source' field.
 
    Sources are pipe-delimited when multiple devices contributed.
    Returns an empty list if the field is absent or blank.
    """
    raw = point.get("source", "")
    if not raw:
        return []
    return [s.strip() for s in raw.split("|") if s.strip()]


# ---------------------------------------------------------------------------
# Metric handlers
# ---------------------------------------------------------------------------

def handle_standard_metric(display_name: str, units: str, data: list[dict]):
    """Handle metrics with a simple `qty` field per data point.

    Buckets data points into INTERVAL_MINUTES windows and aggregates
    according to METRIC_AGGREGATION rules.
    """
    # bucket_ts -> list of qty values
    buckets: dict[int, list[float]] = defaultdict(list)
    bucket_sources: dict[int, set[str]] = defaultdict(set)

    for point in data:
        qty = point.get("qty")
        if qty is None:
            continue
        try:
            unix_ts = parse_unix(point["date"])
        except (KeyError, ValueError) as e:
            logger.warning("Skipping data point for %s - bad date: %s", display_name, e)
            continue
        bucket_ts = bucket_timestamp(unix_ts, INTERVAL_MINUTES)
        buckets[bucket_ts].append(float(qty))
        bucket_sources[bucket_ts].update(_parse_sources(point))

    agg_type = _get_agg_type(display_name, display_name)

    for bucket_ts, values in buckets.items():
        aggregated_value = _aggregate(values, agg_type)
        value_json = json.dumps({f"{display_name} ({units})": aggregated_value})
        insert_health_entry(bucket_ts, display_name, value_json, list(bucket_sources[bucket_ts]))


def handle_heart_rate(display_name: str, data: list[dict]):
    """Handle Heart Rate metric which has Min/Avg/Max fields per data point."""
    # bucket_ts -> {"Min": [...], "Avg": [...], "Max": [...]}
    buckets: dict[int, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    bucket_sources: dict[int, set[str]] = defaultdict(set)

    for point in data:
        try:
            unix_ts = parse_unix(point["date"])
        except (KeyError, ValueError) as e:
            logger.warning("Skipping heart rate point - bad date: %s", e)
            continue
        bucket_ts = bucket_timestamp(unix_ts, INTERVAL_MINUTES)
        for field in ("Min", "Avg", "Max"):
            val = point.get(field)
            if val is not None:
                buckets[bucket_ts][field].append(float(val))
        bucket_sources[bucket_ts].update(_parse_sources(point))

    for bucket_ts, field_values in buckets.items():
        aggregated = {}
        if "Min" in field_values:
            aggregated["Min (count/min)"] = min(field_values["Min"])
        if "Avg" in field_values:
            aggregated["Avg (count/min)"] = statistics.mean(field_values["Avg"])
        if "Max" in field_values:
            aggregated["Max (count/min)"] = max(field_values["Max"])
        if aggregated:
            insert_health_entry(bucket_ts, display_name, json.dumps(aggregated), list(bucket_sources[bucket_ts]))


def handle_sleep_analysis(display_name: str, data: list[dict]):
    """Handle unaggregated Sleep Analysis segments.

    Each data point has startDate/endDate and a sleep stage value.
    Stored as a duration entry bucketed to startDate, with end timestamp
    and stage in value_json.
    """
    for point in data:
        try:
            start_unix = parse_unix(point["startDate"])
            end_unix = parse_unix(point["endDate"])
        except (KeyError, ValueError) as e:
            logger.warning("Skipping sleep segment - bad date: %s", e)
            continue

        bucket_ts = bucket_timestamp(start_unix, INTERVAL_MINUTES)
        qty = point.get("qty")
        stage = point.get("value", "Unspecified")
        sources = _parse_sources(point)

        value_json = json.dumps({
            "end": end_unix,
            "Sleep Analysis (hr)": float(qty) if qty is not None else None,
            "stage": stage,
        })
        insert_health_entry(bucket_ts, display_name, value_json, sources)


def handle_special_qty(display_name: str, units: str, data: list[dict], extra_field: str):
    """Handle metrics that have a qty value plus one categorical extra field.

    Used for: Handwashing (value).

    These are not bucketed/aggregated -- each event is stored individually
    since the categorical field would be lost in aggregation.
    """
    for point in data:
        try:
            unix_ts = parse_unix(point["date"])
        except (KeyError, ValueError) as e:
            logger.warning("Skipping %s point - bad date: %s", display_name, e)
            continue
        bucket_ts = bucket_timestamp(unix_ts, INTERVAL_MINUTES)
        qty = point.get("qty")
        extra = point.get(extra_field)
        sources = _parse_sources(point)
        value_json = json.dumps({
            f"{display_name} ({units})": float(qty) if qty is not None else None,
            extra_field: extra,
        })
        insert_health_entry(bucket_ts, display_name, value_json, sources)


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

# Metrics that need a dedicated handler rather than handle_standard_metric.
# Maps snake_case name -> handler identifier.
SPECIAL_HANDLERS = {
    "heart_rate",
    "sleep_analysis",
    "handwashing",
}

# Normalise known HAE aliases to canonical snake_case names before dispatch.
# Add entries here if HAE sends a name that differs from the expected snake_case.
SNAKE_ALIASES: dict[str, str] = {
    "basal_energy_burned": "resting_energy",
}


def _dispatch(snake_name: str, units: str, data: list[dict]) -> None:
    """Route a single metric to its dedicated handler.

    Applies SNAKE_ALIASES normalisation first, then dispatches to a special
    handler (heart_rate, sleep_analysis, handwashing) or the generic
    handle_standard_metric for everything else.  Logs a warning and returns
    without error for unknown metric names.
    """
    snake_name = SNAKE_ALIASES.get(snake_name, snake_name)
    display_name = SNAKE_TO_DISPLAY.get(snake_name)
    if display_name is None:
        logger.warning("Unknown metric '%s' - no display name mapping, skipping.", snake_name)
        return

    if snake_name == "heart_rate":
        handle_heart_rate(display_name, data)
    elif snake_name == "sleep_analysis":
        handle_sleep_analysis(display_name, data)
    elif snake_name == "handwashing":
        handle_special_qty(display_name, units, data, "value")
    else:
        handle_standard_metric(display_name, units, data)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def handle_health_upload(data: dict[str, Any]) -> None:
    """Process a full Health Auto Export payload and insert each metric into the DB.

    Iterates over the 'metrics' list, dispatches each to the appropriate handler
    (standard, heart-rate, sleep, etc.) and logs a summary on completion.
    Errors in individual metrics are caught and logged so one bad metric does not
    abort the rest of the upload.
    """
    logger.upload("Processing health data in the background...")

    metrics = data.get("metrics")
    if not metrics:
        logger.error("No 'metrics' key found in health payload.")
        return

    processed = 0
    skipped = 0

    for metric_obj in metrics:
        snake_name = metric_obj.get("name")
        units = metric_obj.get("units", "")
        points = metric_obj.get("data", [])

        if not snake_name:
            logger.warning("Metric object missing 'name' field, skipping.")
            skipped += 1
            continue

        if not points:
            logger.upload("Metric '%s' has no data points, skipping.", snake_name)
            skipped += 1
            continue

        logger.upload("Processing metric '%s' (%d points)...", snake_name, len(points))
        try:
            _dispatch(snake_name, units, points)
            processed += 1
        except Exception as e:
            logger.error("Error processing metric '%s': %s", snake_name, e, exc_info=True)
            skipped += 1

    logger.upload(
        f"Finished processing health data: {processed} metrics processed, {skipped} skipped."
    )


# ---------------------------------------------------------------------------
# Aggregation helpers
# ---------------------------------------------------------------------------

def _get_agg_type(metric: str, sub_metric: str) -> str:
    """Return the aggregation type ('sum', 'min', 'max', 'mean') for a metric column.

    Falls back to the first rule for the metric if sub_metric isn't found,
    then falls back to 'mean' if the metric itself isn't in METRIC_AGGREGATION.
    """
    if metric not in METRIC_AGGREGATION:
        return "mean"
    rules = METRIC_AGGREGATION[metric]
    if sub_metric in rules:
        return rules[sub_metric]
    if rules:
        return next(iter(rules.values()))
    return "mean"


def _aggregate(values: list[float], agg_type: str) -> float:
    """Reduce a list of floats using the given aggregation type."""
    if agg_type == "sum":
        return sum(values)
    if agg_type == "min":
        return min(values)
    if agg_type == "max":
        return max(values)
    return statistics.mean(values)