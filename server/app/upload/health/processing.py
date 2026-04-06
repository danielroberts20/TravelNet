from collections import defaultdict
from datetime import datetime
import io
import logging
import statistics
from typing import Any

import pandas as pd  # type: ignore

from config.general import DATA_DIR, INTERVAL_MINUTES
from upload.health.constants import METRIC_AGGREGATION, SNAKE_TO_DISPLAY
from database.health.table import insert_health_quantity
from database.health.heart_rate.table import insert_heart_rate
from database.health.sleep.table import insert_sleep_stage

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
        for source in (list(bucket_sources[bucket_ts]) or [None]):
            insert_health_quantity(bucket_ts, display_name, aggregated_value, units, source)


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
        min_bpm = min(field_values["Min"]) if "Min" in field_values else None
        avg_bpm = statistics.mean(field_values["Avg"]) if "Avg" in field_values else None
        max_bpm = max(field_values["Max"]) if "Max" in field_values else None
        if min_bpm is not None and avg_bpm is not None and max_bpm is not None:
            for source in (list(bucket_sources[bucket_ts]) or [None]):
                insert_heart_rate(bucket_ts, min_bpm, avg_bpm, max_bpm, source)


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

        qty = point.get("qty")
        stage = point.get("value", "Unspecified")
        sources = _parse_sources(point)
        duration_hr = float(qty) if qty is not None else 0.0

        for source in (sources or [None]):
            insert_sleep_stage(start_unix, end_unix, stage, duration_hr, source)


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
        value = float(qty) if qty is not None else None
        if value is not None:
            for source in (sources or [None]):
                insert_health_quantity(bucket_ts, display_name, value, units, source)


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

# Metrics that need a dedicated handler rather than handle_standard_metric.
SPECIAL_HANDLERS = {
    "heart_rate",
    "sleep_analysis",
    "handwashing",
}

# Normalise known HAE aliases to canonical snake_case names before dispatch.
SNAKE_ALIASES: dict[str, str] = {
    "basal_energy_burned": "resting_energy",
}


def _dispatch(snake_name: str, units: str, data: list[dict]) -> None:
    """Route a single metric to its dedicated handler."""
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
# Entry points
# ---------------------------------------------------------------------------

def handle_health_upload(data: dict[str, Any]) -> None:
    """Process a full Health Auto Export payload and insert each metric into the DB."""
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
# CSV/DataFrame processing (moved from models/parsers.py)
# ---------------------------------------------------------------------------

def generate_full_day_index(day: str, interval_minutes: int = INTERVAL_MINUTES) -> pd.DatetimeIndex:
    """Return a DatetimeIndex covering the full day at interval_minutes resolution."""
    return pd.date_range(
        start=f"{day} 00:00:00",
        end=f"{day} 23:59:59",
        freq=f"{interval_minutes}min"
    )

def aggregate_metric_from_csv(
    metric_name: str,
    csv_string: str,
    full_index: pd.DatetimeIndex,
    agg_config: dict
):
    """Resample a point-in-time metric CSV into a full-day interval DataFrame."""
    empty_df = pd.DataFrame(index=full_index)

    if not csv_string.strip():
        return empty_df

    df = pd.read_csv(io.StringIO(csv_string), parse_dates=[0])
    df = df.convert_dtypes()

    if df.empty:
        return empty_df

    ts_col = df.columns[0]
    df[ts_col] = pd.to_datetime(df[ts_col])
    df = df.set_index(ts_col)

    metric_rules = agg_config.get(metric_name, {})

    col_aggs = {}
    for col in df.columns:
        col_aggs[col] = metric_rules.get(col, "mean")

    resampled = df.resample(
        full_index.freq,
        origin="start_day"
    ).agg(col_aggs)

    resampled = resampled.reindex(full_index)
    resampled = resampled.add_prefix(f"{metric_name}_")

    return resampled

def process_interval_metric(
    metric_name: str,
    csv_string: str,
    full_index: pd.DatetimeIndex
):
    """Expand a Start/End interval metric CSV into a full-day interval DataFrame."""
    if not csv_string.strip():
        return pd.DataFrame(index=full_index)

    df = pd.read_csv(io.StringIO(csv_string), parse_dates=["Start", "End"])
    df = df.convert_dtypes()

    if df.empty:
        return pd.DataFrame(index=full_index)

    df["Start"] = pd.to_datetime(df["Start"])
    df["End"] = pd.to_datetime(df["End"])

    output = pd.DataFrame(index=full_index)

    interval_minutes = int(full_index.freq.delta.total_seconds() / 60)

    for _, row in df.iterrows():
        start = row["Start"]
        end = row["End"]

        day_start = full_index[0]
        day_end = full_index[-1] + full_index.freq

        start = max(start, day_start)
        end = min(end, day_end)

        if start >= end:
            continue

        mask = (full_index < end) & (full_index + full_index.freq > start)
        overlapping_intervals = full_index[mask]

        for col in df.columns:
            if col in ["Start", "End"]:
                continue

            prefixed_col = f"{metric_name}_{col}"

            if prefixed_col not in output.columns:
                output[prefixed_col] = pd.NA

            output.loc[overlapping_intervals, prefixed_col] = row[col]

    return output

def process_payload(payload: dict, day: str):
    """Join all metric DataFrames for a day and write the result to processed.csv."""
    full_index = generate_full_day_index(day, INTERVAL_MINUTES)

    final_df = pd.DataFrame(index=full_index)

    for metric_name, csv_string in payload.items():
        logger.info(f"Processing metric: {metric_name}")
        if "Start" in csv_string and "End" in csv_string:
            df_metric = process_interval_metric(
                metric_name,
                csv_string,
                full_index
            )
        else:
            df_metric = aggregate_metric_from_csv(
                metric_name,
                csv_string,
                full_index,
                METRIC_AGGREGATION
            )

        final_df = final_df.join(df_metric, how="left")

    final_df = final_df.reset_index().rename(columns={"index": "Date/Time"})
    final_df.to_csv(DATA_DIR / "processed.csv", index=False)
    return final_df


# ---------------------------------------------------------------------------
# Aggregation helpers
# ---------------------------------------------------------------------------

def _get_agg_type(metric: str, sub_metric: str) -> str:
    """Return the aggregation type ('sum', 'min', 'max', 'mean') for a metric column."""
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
