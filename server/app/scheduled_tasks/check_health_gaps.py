"""
Daily health data gap detection and upload recency check.
Scheduled: Every day at 09:00.

  - Checks whether any health data has landed in the DB in the last
    HEALTH_UPLOAD_STALE_HOURS hours. Fires a Pushcut ERROR notification
    on first detection and again every 24h if still stale.
  - Scans health_quantity, health_heart_rate, and health_sleep for
    significant gaps using adaptive median-cadence thresholds.
  - Sends a plain-text email report if any gaps are found.
  - Silent if everything is healthy.
"""

from __future__ import annotations

import json
import statistics
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from prefect import flow, task, get_run_logger
from config.editable import load_overrides

load_overrides()

from config.general import (
    HEALTH_GAP_LOOKBACK_DAYS,
    HEALTH_GAP_MIN_HOURS,
    HEALTH_GAP_MULTIPLIER,
    HEALTH_MIN_HISTORY_POINTS,
    HEALTH_MIN_POINTS_AFTER,
    HEALTH_UPLOAD_STALE_HOURS,
    HEALTH_STALE_ALERT_FILE,
)
from config.settings import settings
from database.connection import get_conn
from notifications import error_notification, notify_on_completion, log_on_success, record_flow_result, send_email


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class GapFinding:
    domain: str
    gap_start: str
    gap_end: str
    gap_hours: float
    median_cadence_hours: float
    ratio: float
    tentative: bool
    points_after: int
    trailing: bool = False


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _to_dt(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def _gap_threshold(median_h: float) -> float:
    return max(HEALTH_GAP_MIN_HOURS, HEALTH_GAP_MULTIPLIER * median_h)


def _scan_timestamps(
    timestamps: list[datetime],
    domain: str,
    count_after_fn,
    median_h: float,
    now: datetime,
) -> list[GapFinding]:
    findings: list[GapFinding] = []
    threshold_h = _gap_threshold(median_h)

    for i in range(1, len(timestamps)):
        gap_h = (timestamps[i] - timestamps[i - 1]).total_seconds() / 3600
        if gap_h < threshold_h:
            continue
        gap_start_str = timestamps[i - 1].strftime("%Y-%m-%dT%H:%M:%SZ")
        gap_end_str = timestamps[i].strftime("%Y-%m-%dT%H:%M:%SZ")
        points_after = count_after_fn(gap_end_str)
        findings.append(GapFinding(
            domain=domain,
            gap_start=gap_start_str,
            gap_end=gap_end_str,
            gap_hours=round(gap_h, 2),
            median_cadence_hours=round(median_h, 3),
            ratio=round(gap_h / median_h, 1),
            tentative=points_after < HEALTH_MIN_POINTS_AFTER,
            points_after=points_after,
        ))

    if timestamps:
        trailing_h = (now - timestamps[-1]).total_seconds() / 3600
        if trailing_h >= threshold_h:
            findings.append(GapFinding(
                domain=domain,
                gap_start=timestamps[-1].strftime("%Y-%m-%dT%H:%M:%SZ"),
                gap_end=_now_str(),
                gap_hours=round(trailing_h, 2),
                median_cadence_hours=round(median_h, 3),
                ratio=round(trailing_h / median_h, 1),
                tentative=True,
                points_after=0,
                trailing=True,
            ))

    return findings


# ---------------------------------------------------------------------------
# Suppression helpers
# ---------------------------------------------------------------------------

def _load_stale_alert_state(path: Path) -> datetime | None:
    """Return the timestamp of the last stale alert, or None if never fired."""
    try:
        data = json.loads(path.read_text())
        return _to_dt(data["alerted_at"])
    except (FileNotFoundError, KeyError, ValueError):
        return None


def _save_stale_alert_state(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"alerted_at": _now_str()}))


def _clear_stale_alert_state(path: Path) -> None:
    path.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(name="check_health_upload_recency")
def check_health_upload_recency() -> dict:
    """Fire a Pushcut ERROR if no health data has landed recently.

    Suppressed for 24h after first alert to avoid repeated notifications
    for the same outage. Re-fires every 24h if still stale.
    """
    log = get_run_logger()
    now = datetime.now(timezone.utc)

    conn = get_conn(read_only=True)
    try:
        latest_quantity = conn.execute(
            "SELECT MAX(timestamp) FROM health_quantity"
        ).fetchone()[0]
        latest_hr = conn.execute(
            "SELECT MAX(timestamp) FROM health_heart_rate"
        ).fetchone()[0]
    finally:
        conn.close()

    candidates = [ts for ts in (latest_quantity, latest_hr) if ts]
    if not candidates:
        stale_hours = None
        stale = True
        log.warning("No health data in DB at all")
    else:
        latest = max(_to_dt(ts) for ts in candidates)
        stale_hours = round((now - latest).total_seconds() / 3600, 1)
        stale = stale_hours > HEALTH_UPLOAD_STALE_HOURS
        log.info("Most recent health data: %s (%.1fh ago)", latest.isoformat(), stale_hours)

    if stale:
        last_alerted = _load_stale_alert_state(HEALTH_STALE_ALERT_FILE)
        suppress = (
            last_alerted is not None
            and (now - last_alerted).total_seconds() < 86400
        )
        if not suppress:
            msg = (
                f"No health data received for {stale_hours:.1f}h — HAE may have stopped uploading."
                if stale_hours is not None
                else "No health data in DB at all."
            )
            error_notification(msg)
            _save_stale_alert_state(HEALTH_STALE_ALERT_FILE)
            log.warning("Stale upload alert fired: %s", msg)
        else:
            hours_since = round((now - last_alerted).total_seconds() / 3600, 1)
            log.info("Stale upload suppressed — last alert was %.1fh ago", hours_since)
    else:
        _clear_stale_alert_state(HEALTH_STALE_ALERT_FILE)

    return {"stale": stale, "stale_hours": stale_hours}


@task(name="scan_quantity_gaps")
def scan_quantity_gaps() -> list[GapFinding]:
    log = get_run_logger()
    now = datetime.now(timezone.utc)
    cutoff_str = (now - timedelta(days=HEALTH_GAP_LOOKBACK_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")

    conn = get_conn(read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT metric, source, timestamp
            FROM   health_quantity
            WHERE  timestamp >= ?
            ORDER  BY metric, source, timestamp
            """,
            (cutoff_str,),
        ).fetchall()
    finally:
        conn.close()

    from collections import defaultdict
    groups: dict[tuple[str, str], list[datetime]] = defaultdict(list)
    for row in rows:
        try:
            groups[(row["metric"], row["source"])].append(_to_dt(row["timestamp"]))
        except ValueError:
            pass

    all_findings: list[GapFinding] = []

    for (metric, source), timestamps in groups.items():
        if len(timestamps) < HEALTH_MIN_HISTORY_POINTS:
            log.debug("Skipping %r/%r: only %d points in lookback window", metric, source, len(timestamps))
            continue

        timestamps.sort()
        diffs_h = [
            (timestamps[i] - timestamps[i - 1]).total_seconds() / 3600
            for i in range(1, len(timestamps))
        ]
        median_h = statistics.median(diffs_h)
        if median_h < 1e-4:
            continue

        def count_after(gap_end_str: str, _metric=metric) -> int:
            c = get_conn(read_only=True)
            try:
                return c.execute(
                    "SELECT COUNT(*) FROM health_quantity WHERE metric = ? AND timestamp > ?",
                    (_metric, gap_end_str),
                ).fetchone()[0]
            finally:
                c.close()

        all_findings.extend(_scan_timestamps(timestamps, f"quantity:{metric}", count_after, median_h, now))

    log.info("Quantity: %d gap(s) found across all metrics", len(all_findings))
    return all_findings


@task(name="scan_heart_rate_gaps")
def scan_heart_rate_gaps() -> list[GapFinding]:
    log = get_run_logger()
    now = datetime.now(timezone.utc)
    cutoff_str = (now - timedelta(days=HEALTH_GAP_LOOKBACK_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")

    conn = get_conn(read_only=True)
    try:
        rows = conn.execute(
            "SELECT timestamp FROM health_heart_rate WHERE timestamp >= ? ORDER BY timestamp",
            (cutoff_str,),
        ).fetchall()
    finally:
        conn.close()

    if len(rows) < HEALTH_MIN_HISTORY_POINTS:
        get_run_logger().info("Heart rate: only %d points in lookback window — skipping", len(rows))
        return []

    timestamps = []
    for row in rows:
        try:
            timestamps.append(_to_dt(row["timestamp"]))
        except ValueError:
            pass

    diffs_h = [(timestamps[i] - timestamps[i - 1]).total_seconds() / 3600 for i in range(1, len(timestamps))]
    median_h = statistics.median(diffs_h)

    def count_after(gap_end_str: str) -> int:
        c = get_conn(read_only=True)
        try:
            return c.execute(
                "SELECT COUNT(*) FROM health_heart_rate WHERE timestamp > ?",
                (gap_end_str,),
            ).fetchone()[0]
        finally:
            c.close()

    findings = _scan_timestamps(timestamps, "heart_rate", count_after, median_h, now)
    get_run_logger().info("Heart rate: %d gap(s) found", len(findings))
    return findings


@task(name="scan_sleep_gaps")
def scan_sleep_gaps() -> list[GapFinding]:
    log = get_run_logger()
    now = datetime.now(timezone.utc)
    today = now.date()
    cutoff_str = (now - timedelta(days=HEALTH_GAP_LOOKBACK_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")

    conn = get_conn(read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT DATE(start_ts) AS night_date
            FROM   health_sleep
            WHERE  start_ts >= ?
            GROUP  BY DATE(start_ts)
            ORDER  BY night_date
            """,
            (cutoff_str,),
        ).fetchall()
        total_sleep_rows = conn.execute(
            "SELECT COUNT(*) FROM health_sleep"
        ).fetchone()[0]
    finally:
        conn.close()

    if not rows:
        log.info("Sleep: no data in lookback window")
        return []

    recorded_nights: set[date] = {
        datetime.strptime(r["night_date"], "%Y-%m-%d").date()
        for r in rows
    }

    findings: list[GapFinding] = []
    first_night = min(recorded_nights)
    last_night = max(recorded_nights)
    gap_start: date | None = None
    night = first_night

    while night <= last_night:
        if night not in recorded_nights and night < today:
            if gap_start is None:
                gap_start = night
        else:
            if gap_start is not None:
                gap_start_dt = datetime(gap_start.year, gap_start.month, gap_start.day, tzinfo=timezone.utc)
                gap_end_dt = datetime(night.year, night.month, night.day, tzinfo=timezone.utc)
                gap_h = (gap_end_dt - gap_start_dt).total_seconds() / 3600
                findings.append(GapFinding(
                    domain="sleep",
                    gap_start=gap_start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    gap_end=gap_end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    gap_hours=round(gap_h, 1),
                    median_cadence_hours=24.0,
                    ratio=round(gap_h / 24.0, 1),
                    tentative=total_sleep_rows < HEALTH_MIN_POINTS_AFTER,
                    points_after=total_sleep_rows,
                ))
                gap_start = None
        night += timedelta(days=1)

    log.info("Sleep: %d gap(s) found", len(findings))
    return findings


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(name="Check Health Gaps", on_failure=[notify_on_completion], on_completion=[log_on_success])
def check_health_gaps_flow() -> dict:
    """
    Daily health monitor. Checks upload recency first, then scans for gaps.
    Pushcut alert if HAE has stopped uploading. Email report if gaps are found.
    Silent if everything is healthy.
    """
    log = get_run_logger()

    recency = check_health_upload_recency()
    quantity_findings = scan_quantity_gaps()
    hr_findings = scan_heart_rate_gaps()
    sleep_findings = scan_sleep_gaps()

    all_findings: list[GapFinding] = quantity_findings + hr_findings + sleep_findings
    all_findings.sort(key=lambda f: f.gap_hours, reverse=True)

    result = {
        "upload_stale": recency["stale"],
        "stale_hours": recency["stale_hours"],
        "gaps_found": len(all_findings),
    }

    if not all_findings:
        log.info("No health gaps detected — no email sent")
        record_flow_result(result)
        return result

    confirmed = [f for f in all_findings if not f.tentative]
    tentative = [f for f in all_findings if f.tentative]

    def _fmt(f: GapFinding) -> str:
        tag_parts = []
        if f.trailing:
            tag_parts.append("trailing — extends to now")
        if f.tentative and not f.trailing:
            tag_parts.append(f"only {f.points_after} record(s) follow")
        tag = f"  [{', '.join(tag_parts)}]" if tag_parts else ""
        return (
            f"  {f.domain}\n"
            f"    start : {f.gap_start}\n"
            f"    end   : {f.gap_end}\n"
            f"    gap   : {f.gap_hours:.1f}h  ({f.ratio:.1f}× median {f.median_cadence_hours:.2f}h){tag}"
        )

    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    lines = [
        f"Health gap check — {run_date}",
        f"Lookback: {HEALTH_GAP_LOOKBACK_DAYS}d  |  Threshold: {HEALTH_GAP_MULTIPLIER}× median cadence "
        f"(floor {HEALTH_GAP_MIN_HOURS}h)  |  Min cadence points: {HEALTH_MIN_HISTORY_POINTS}",
        f"Found {len(confirmed)} confirmed gap(s) and {len(tentative)} tentative gap(s).",
        "",
    ]

    if confirmed:
        lines.append("CONFIRMED GAPS")
        lines.append("-" * 50)
        for f in confirmed:
            lines.append(_fmt(f))
            lines.append("")

    if tentative:
        lines.append("TENTATIVE GAPS  (few or no records follow — data may still be incoming)")
        lines.append("-" * 50)
        for f in tentative:
            lines.append(_fmt(f))
            lines.append("")

    body = "\n".join(lines)
    log.info("Gap report:\n%s", body)

    subject = (
        f"[TravelNet] Health gaps — {len(confirmed)} confirmed, {len(tentative)} tentative"
        if tentative
        else f"[TravelNet] Health gaps — {len(confirmed)} found"
    )
    send_email(subject=subject, body=body, **settings.smtp_config)
    log.info("Gap report sent (%d total findings)", len(all_findings))

    record_flow_result(result)
    return result