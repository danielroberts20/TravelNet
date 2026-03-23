from datetime import datetime
import io
import json
import logging
from typing import Any
import pandas as pd # type: ignore
from config.general import INTERVAL_MINUTES, DATA_DIR
from upload.health.constants import METRIC_AGGREGATION

logger = logging.getLogger(__name__)

def parse_int(value: str | None) -> int | None:
    """Parse a string to int, returning None for empty/None values."""
    if value in ("", None):
        return None
    return int(value)

def parse_float(value: str | None) -> float | None:
    """Parse a string to float, returning None for empty/None values."""
    if value in ("", None):
        return None
    return float(value)

def parse_bool_yes_no(value: str) -> bool:
    """Parse a 'Yes'/'No' string to bool."""
    return value.lower() == "yes"

def parse_string(value: str | None) -> str | None:
    """Return the string unchanged, or None for empty/None values."""
    if value in ("", None):
        return None
    return value

def parse_cellular_states(states: str | None):
    """Parse a JSON-encoded list of cellular state dicts into CellularState objects."""
    from telemetry_models import CellularState

    if states in ("", None):
        return None
    try:
        return [
            CellularState.from_json(**i)
            for i in json.loads(states)
        ]
    except Exception as e:
        logger.warning(f"Bad cellular data\t Cellular entry: {states}\tException: {e}")
        return None

def handle_health_upload(data: dict[str, Any]):
    """Process a health payload for today's date (legacy entry point)."""
    process_payload(data, datetime.now().strftime("%Y-%m-%d"))

def generate_full_day_index(day: str, interval_minutes: int=INTERVAL_MINUTES) -> pd.DatetimeIndex:
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
    # Always build empty frame first (guarantees column presence)
    empty_df = pd.DataFrame(index=full_index)

    if not csv_string.strip():
        # No data → return empty columns but correct index
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

    # Reindex to full day (forces all intervals)
    resampled = resampled.reindex(full_index)

    # Prefix columns
    resampled = resampled.add_prefix(f"{metric_name}_")

    return resampled

def process_interval_metric(
    metric_name: str,
    csv_string: str,
    full_index: pd.DatetimeIndex
):
    """Expand a Start/End interval metric CSV into a full-day interval DataFrame."""
    if not csv_string.strip():
        # Return empty frame with correct index
        return pd.DataFrame(index=full_index)

    df = pd.read_csv(io.StringIO(csv_string), parse_dates=["Start", "End"])
    df = df.convert_dtypes()

    if df.empty:
        return pd.DataFrame(index=full_index)

    df["Start"] = pd.to_datetime(df["Start"])
    df["End"] = pd.to_datetime(df["End"])

    # Create empty output frame
    output = pd.DataFrame(index=full_index)

    interval_minutes = int(full_index.freq.delta.total_seconds() / 60)

    for _, row in df.iterrows():
        start = row["Start"]
        end = row["End"]

        # Clip to day boundaries
        day_start = full_index[0]
        day_end = full_index[-1] + full_index.freq

        start = max(start, day_start)
        end = min(end, day_end)

        if start >= end:
            continue

        # Determine which buckets overlap
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

def process_payload(payload: dict,
                    day: str):
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