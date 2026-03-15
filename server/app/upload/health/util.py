from collections import defaultdict
import csv
from datetime import datetime
import io
import json
import logging
import statistics
from typing import Any

from config.general import INTERVAL_MINUTES, METRIC_AGGREGATION, METRICS
from database.health.table import insert_health_entry

logger = logging.getLogger(__name__)

def handle_duration_metric(current_metric: str, header: list[str], reader: csv.reader, timezone: str):
    start = header.index("Start")
    end = header.index("End")
    source = header.index("Source")
    for row in reader:
        start_ts = datetime.strptime(f"{row[start]}{timezone}", "%Y-%m-%d %H:%M:%S%z")
        end_ts = datetime.strptime(f"{row[end]}{timezone}", "%Y-%m-%d %H:%M:%S%z")
        unix_start = floor_to_interval(int(start_ts.timestamp()), INTERVAL_MINUTES)
        unix_end = int(end_ts.timestamp())
        value_json = {"end": unix_end}
        for i in range(len(header)):
            if i not in [start, end, source]:
                value_json[header[i]] = row[i]
        insert_health_entry(
            unix_start,
            current_metric,
            json.dumps(value_json),
            row[source].split("|")
        )

def handle_health_upload(data: dict[str, Any]):
    logger.info("Processing health data in the background...")

    timezone = data.get("timezone")
    if not timezone:
        logger.error("Timezone not provided.")
        return

    data.pop("timezone")

    for current_metric in METRICS:
        logger.info(f"Processing metric {current_metric}...")
        if current_metric not in data:
            logger.warning(f"{current_metric} not found in uploaded data.")
            continue

        csv_text = data[current_metric]
        r = csv.reader(io.StringIO(csv_text))
        header = next(r)
        if header[0] != "Date/Time":
            if header[0] == "Start":
                logger.info(f"Duration metric {current_metric} detected based on header. Processing with duration handler.")
                handle_duration_metric(current_metric, header, r, timezone)
            else:
                logger.warn(f"Unexpected header format for metric {current_metric}. Expected first column to be 'Date/Time'.")
            continue

        sub_metrics = header[1:-1]

        # bucket -> submetric -> list of values
        buckets = defaultdict(lambda: defaultdict(list))
        bucket_sources = defaultdict(set)

        for row in r:
            dt = datetime.strptime(f"{row[0]}{timezone}", "%Y-%m-%d %H:%M:%S%z")
            unix_ts = int(dt.timestamp())
            bucket_ts = bucket_timestamp(unix_ts, INTERVAL_MINUTES)

            for i, sub_metric in enumerate(sub_metrics):
                if row[i+1] != "":
                    value = float(row[i+1])
                    buckets[bucket_ts][sub_metric].append(value)
            
            sources = row[-1].split("|")
            bucket_sources[bucket_ts].update(sources)
        
        # Now aggregate per bucket
        for bucket_ts, submetric_values in buckets.items():
            aggregated = {}

            for sub_metric, values in submetric_values.items():
                agg_type = get_aggregation_type(current_metric, sub_metric)

                if agg_type == "sum":
                    aggregated[sub_metric] = sum(values)
                elif agg_type == "min":
                    aggregated[sub_metric] = min(values)
                elif agg_type == "max":
                    aggregated[sub_metric] = max(values)
                else:  # mean default
                    aggregated[sub_metric] = statistics.mean(values)

            insert_health_entry(
                bucket_ts,
                current_metric,
                json.dumps(aggregated),
                list(bucket_sources[bucket_ts])
            )

    logger.info("Finished processing health data.")        

def get_aggregation_type(metric: str, sub_metric: str) -> str:
    # Metric not defined → mean
    if metric not in METRIC_AGGREGATION:
        return "mean"

    metric_rules = METRIC_AGGREGATION[metric]

    # Exact submetric rule exists
    if sub_metric in metric_rules:
        return metric_rules[sub_metric]

    # If submetric missing → use first available rule in metric
    if metric_rules:
        return next(iter(metric_rules.values()))

    return "mean"

def bucket_timestamp(unix_ts: int, interval_minutes: int) -> int:
    interval_seconds = interval_minutes * 60
    return (unix_ts // interval_seconds) * interval_seconds

def floor_to_interval(unix_ts: int, interval_minutes: int) -> int:
    interval_seconds = interval_minutes * 60
    return (unix_ts // interval_seconds) * interval_seconds