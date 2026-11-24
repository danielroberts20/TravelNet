"""
health_gaps/runner.py
~~~~~~~~~~~~~~~~~~~~~
Glue between the DB, the pure detector, and the state machine. Kept free of
Prefect so it can be exercised directly in tests; the flow in
scheduled_tasks/check_health_gaps.py supplies the connections and the
send_email / push callables.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Callable, Optional

from health_gaps import events as ev_mod
from health_gaps.config import HEART_RATE, SLEEP, monitored_metrics
from health_gaps.detect import (
    Gap, cluster_gaps, detect_metric_gaps, detect_sleep_gaps, fmt, parse,
)
from health_gaps.events import Params, Report
from health_gaps.report import build_body, push_text, should_email, subject


@dataclass
class HealthData:
    series: dict[str, list[datetime]] = field(default_factory=dict)
    last_seen: dict[str, datetime] = field(default_factory=dict)
    sleep_nights: dict[date, int] = field(default_factory=dict)


def compute_scan_start(state_conn: sqlite3.Connection, now: datetime, lookback_days: float) -> datetime:
    """Normal lookback, stretched back to cover any unresolved event."""
    start = now - timedelta(days=lookback_days)
    oldest = ev_mod.earliest_live_start(state_conn)
    if oldest is not None:
        start = min(start, oldest - timedelta(days=1))
    return start


def load_health_data(conn: sqlite3.Connection, scan_start: datetime) -> HealthData:
    data = HealthData()
    cutoff = fmt(scan_start)
    metrics = monitored_metrics()

    marks = ",".join("?" * len(metrics))
    for row in conn.execute(
        f"SELECT metric, timestamp FROM health_quantity "
        f"WHERE timestamp >= ? AND metric IN ({marks}) ORDER BY metric, timestamp",
        (cutoff, *metrics),
    ):
        try:
            data.series.setdefault(row[0], []).append(parse(row[1]))
        except ValueError:
            pass
    for m in metrics:
        last = conn.execute("SELECT MAX(timestamp) FROM health_quantity WHERE metric = ?", (m,)).fetchone()[0]
        if last:
            data.last_seen[m] = parse(last)

    hr = [parse(r[0]) for r in conn.execute(
        "SELECT timestamp FROM health_heart_rate WHERE timestamp >= ? ORDER BY timestamp", (cutoff,))]
    data.series[HEART_RATE] = hr
    last_hr = conn.execute("SELECT MAX(timestamp) FROM health_heart_rate").fetchone()[0]
    if last_hr:
        data.last_seen[HEART_RATE] = parse(last_hr)

    for d, n in conn.execute(
        "SELECT DATE(start_ts), COUNT(*) FROM health_sleep WHERE start_ts >= ? GROUP BY DATE(start_ts)",
        (cutoff,),
    ):
        data.sleep_nights[date.fromisoformat(d)] = n
    if not data.sleep_nights:
        last = conn.execute("SELECT MAX(DATE(start_ts)) FROM health_sleep").fetchone()[0]
        if last:
            data.sleep_nights[date.fromisoformat(last)] = 1
    return data


def find_gaps(data: HealthData, now: datetime, min_nights: int) -> list[Gap]:
    gaps: list[Gap] = []
    for metric in set(data.series) | set(data.last_seen):
        gaps += detect_metric_gaps(metric, data.series.get(metric, []), now, data.last_seen.get(metric))
    gaps += detect_sleep_gaps(data.sleep_nights, now.date(), min_nights)
    return gaps


def run_check(
    state_conn: sqlite3.Connection,
    data: HealthData,
    now: datetime,
    scan_start: datetime,
    *,
    send_email: Callable[[str, str], None],
    push: Callable[[str], None],
    digest_weekday: int = 0,
    min_nights: int = 2,
    params: Params = Params(),
    auto_recovery: bool = False,
) -> Report:
    """Detect, reconcile persisted state, and notify only about what is new."""
    candidates = cluster_gaps(find_gaps(data, now, min_nights), now)
    report = ev_mod.reconcile(state_conn, candidates, now, scan_start, params)
    is_digest_day = now.weekday() == digest_weekday

    for event in report.to_push:
        push(push_text(event))
    ev_mod.mark_pushed(state_conn, [e.id for e in report.to_push], now)

    if should_email(report, is_digest_day):
        body = build_body(report, now, is_digest_day, params.export_pad_h, auto_recovery)
        send_email(subject(report, is_digest_day, auto_recovery), body)
        told = report.newly_confirmed + (report.held_for_digest if is_digest_day else [])
        ev_mod.mark_notified(state_conn, [e.id for e in told], now)
    return report
