"""
health_gaps/config.py
~~~~~~~~~~~~~~~~~~~~~
Per-metric gap-detection config.

Each Health Auto Export metric is given an expected-cadence *class* and an
absolute threshold in hours, instead of a multiple of a rolling median (which
is unstable for metrics only recorded while asleep / walking / worn).

Fields
------
threshold_h  Gap must be at least this long to count.
group        Metrics sharing a group whose gaps overlap in time are merged into
             one "outage" event (e.g. the Watch vitals stopping together).
immediate    True  -> a confirmed gap triggers an email straight away.
             False -> reported in the Monday digest only.
critical     True  -> a confirmed gap also fires a Pushcut notification.
sparse       True  -> recorded too rarely for a gap to justify asking the user to start the
                      HAE server; such a gap is only recovered when a prompt happens anyway.

Metrics not listed here (Resting Heart Rate, Walking Heart Rate Average,
Cardio Recovery, ...) are sparse by nature and are deliberately not monitored.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class MetricConfig:
    cls: str
    threshold_h: float
    group: Optional[str] = None
    immediate: bool = True
    critical: bool = False
    sparse: bool = False   # phone rarely has more than the DB; a gap alone never justifies a recovery prompt


# Virtual metric names (not rows in health_quantity)
HEART_RATE = "heart_rate"
SLEEP = "sleep"

_WATCH_VITALS = "watch_vitals"
_ACTIVITY_TOTALS = "activity_totals"
_WALKING = "walking"

METRIC_CONFIG: dict[str, MetricConfig] = {
    # --- continuous: samples around the clock when the Watch is worn ---------
    HEART_RATE:                          MetricConfig("continuous", 18, _WATCH_VITALS, critical=True),
    "Blood Oxygen Saturation":           MetricConfig("continuous", 18, _WATCH_VITALS),
    "Heart Rate Variability":            MetricConfig("continuous", 18, _WATCH_VITALS),
    "Environmental Audio Exposure":      MetricConfig("continuous", 18, _WATCH_VITALS),
    "Time in Daylight":                  MetricConfig("daylight",   24, _WATCH_VITALS),
    "Step Count":                        MetricConfig("continuous", 18, _ACTIVITY_TOTALS),
    "Walking + Running Distance":        MetricConfig("continuous", 18, _ACTIVITY_TOTALS),
    "Active Energy":                     MetricConfig("continuous", 18, _ACTIVITY_TOTALS),
    "Resting Energy":                    MetricConfig("continuous", 18, _ACTIVITY_TOTALS),
    "Physical Effort":                   MetricConfig("continuous", 18, _ACTIVITY_TOTALS),
    # --- mostly continuous (quiet overnight) ----------------------------------
    "Apple Stand Hour":                  MetricConfig("daytime", 24, _ACTIVITY_TOTALS),
    "Apple Stand Time":                  MetricConfig("daytime", 24, _ACTIVITY_TOTALS),
    "Apple Exercise Time":               MetricConfig("daytime", 24, _ACTIVITY_TOTALS),
    "Flights Climbed":                   MetricConfig("daytime", 24, _ACTIVITY_TOTALS),
    # --- activity-only: iPhone walking metrics, digest only -------------------
    "Walking Speed":                     MetricConfig("activity", 24, _WALKING, immediate=False),
    "Walking Step Length":               MetricConfig("activity", 24, _WALKING, immediate=False),
    "Walking Double Support Percentage": MetricConfig("activity", 24, _WALKING, immediate=False),
    "Walking Asymmetry Percentage":      MetricConfig("activity", 24, _WALKING, immediate=False),
    # Apple only records VO2 Max after qualifying outdoor activity: readings were a median of ~99h
    # apart (49-336h) over the first five months of the trip, so 48h flagged normal behaviour.
    # 7 days leaves only the genuinely long silences.
    "VO2 Max":                           MetricConfig("activity", 168, None, immediate=False, sparse=True),
    # --- sleep-only: written only while asleep ---------------------------------
    "Respiratory Rate":                  MetricConfig("sleep_only", 72, None, immediate=False, sparse=True),
    "Apple Sleeping Wrist Temperature":  MetricConfig("sleep_only", 96, None, immediate=False, sparse=True),
}

# Sleep is nightly, handled as a count of consecutive missing nights.
SLEEP_CONFIG = MetricConfig("nightly", 0, None, immediate=True, critical=True)

# An event involving at least this many metrics is treated as critical.
CRITICAL_METRIC_COUNT = 3


def monitored_metrics() -> list[str]:
    return [m for m in METRIC_CONFIG if m != HEART_RATE]


def config_for(metric: str) -> Optional[MetricConfig]:
    if metric == SLEEP:
        return SLEEP_CONFIG
    return METRIC_CONFIG.get(metric)


def group_key_for(metric: str) -> str:
    cfg = config_for(metric)
    return (cfg.group if cfg and cfg.group else metric)
