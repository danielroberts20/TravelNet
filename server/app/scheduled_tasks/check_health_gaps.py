"""
Scheduled task: scan health tables for significant data gaps.

Algorithm (per metric / table):
  1. Compute the *median* inter-arrival time as the "normal cadence".
  2. Set threshold = max(HEALTH_GAP_MIN_HOURS, HEALTH_GAP_MULTIPLIER × median).
     The 10-hour default floor means overnight sleep gaps (~8h) are never flagged
     for high-frequency metrics (step count, heart rate, etc.).
  3. Flag every gap above the threshold.
  4. Also flag a *trailing gap* from the last record to now (always tentative).
  5. Mark a gap as *tentative* if fewer than HEALTH_MIN_POINTS_AFTER records exist
     after it — a small tail suggests the data feed has only just resumed and more
     is incoming (or the upload is mid-flight).

Sleep uses separate logic: aggregate per night-date, flag calendar nights with no
records at all; a night is only flagged if it pre-dates today.

Registered in deployments.py — no __main__ block.
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

from prefect import flow, task, get_run_logger

from config.editable import load_overrides

load_overrides()

from config.general import (
    HEALTH_GAP_LOOKBACK_DAYS,
    HEALTH_GAP_MIN_HOURS,
    HEALTH_GAP_MULTIPLIER,
    HEALTH_MIN_HISTORY_POINTS,
    HEALTH_MIN_POINTS_AFTER,
)
from config.settings import settings
from database.connection import get_conn
from notifications import send_email


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class GapFinding:
    domain: str            # "quantity:<metric>", "heart_rate", "sleep"
    gap_start: str         # ISO 8601 UTC
    gap_end: str           # ISO 8601 UTC — "now" for trailing gaps
    gap_hours: float
    median_cadence_hours: float
    ratio: float           # gap_hours / median_cadence_hours
    tentative: bool        # True = few records follow; data may still be arriving
    points_after: int      # 0 for trailing gaps, -1 for sleep (not applicable)
    trailing: bool = False # True = gap extends to the present moment


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _to_dt(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def _gap_threshold(median_h: float) -> float:
    """Threshold above which a gap is flagged."""
    return max(HEALTH_GAP_MIN_HOURS, HEALTH_GAP_MULTIPLIER * median_h)


def _scan_timestamps(
    timestamps: list[datetime],
    domain: str,
    count_after_fn,          # callable(gap_end_str) -> int
    median_h: float,
    now: datetime,
) -> list[GapFinding]:
    """
    Core gap detection over a sorted list of timestamps for one metric.

    Checks:
      - Gaps between consecutive records.
      - Trailing gap from the last record to *now*.
    """
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

    # Trailing gap: last record → now
    if timestamps:
        trailing_h = (now - timestamps[-1]).total_seconds() / 3600
        if trailing_h >= threshold_h:
            gap_start_str = timestamps[-1].strftime("%Y-%m-%dT%H:%M:%SZ")
            findings.append(GapFinding(
                domain=domain,
                gap_start=gap_start_str,
                gap_end=_now_str(),
                gap_hours=round(trailing_h, 2),
                median_cadence_hours=round(median_h, 3),
                ratio=round(trailing_h / median_h, 1),
                tentative=True,   # always tentative — no records can follow yet
                points_after=0,
                trailing=True,
            ))

    return findings


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(name="scan_quantity_gaps")
def scan_quantity_gaps() -> list[GapFinding]:
    """
    Scan health_quantity for gaps, grouped by (metric, source).

    Metrics with fewer than HEALTH_MIN_HISTORY_POINTS rows in the lookback
    window are skipped — not enough history to estimate a reliable cadence.
    """
    log = get_run_logger()
    now = datetime.now(timezone.utc)
    cutoff_str = (now - timedelta(days=HEALTH_GAP_LOOKBACK_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")

    # --- single read: fetch all rows in the lookback window ---
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

    # Group by (metric, source)
    from collections import defaultdict
    groups: dict[tuple[str, str], list[datetime]] = defaultdict(list)
    for row in rows:
        try:
            groups[(row["metric"], row["source"])].append(_to_dt(row["timestamp"]))
        except ValueError:
            pass  # malformed timestamp — skip silently

    all_findings: list[GapFinding] = []

    for (metric, source), timestamps in groups.items():
        if len(timestamps) < HEALTH_MIN_HISTORY_POINTS:
            log.debug(
                "Skipping %r/%r: only %d points in lookback window",
                metric, source, len(timestamps),
            )
            continue

        timestamps.sort()
        diffs_h = [
            (timestamps[i] - timestamps[i - 1]).total_seconds() / 3600
            for i in range(1, len(timestamps))
        ]
        median_h = statistics.median(diffs_h)

        if median_h < 1e-4:
            # Guard against near-zero cadence (duplicate timestamps after dedup)
            continue

        # Domain uses metric name; if multiple sources produce gaps, both appear in the report
        domain = f"quantity:{metric}"

        def count_after(gap_end_str: str, _metric=metric) -> int:
            c = get_conn(read_only=True)
            try:
                return c.execute(
                    "SELECT COUNT(*) FROM health_quantity WHERE metric = ? AND timestamp > ?",
                    (_metric, gap_end_str),
                ).fetchone()[0]
            finally:
                c.close()

        findings = _scan_timestamps(timestamps, domain, count_after, median_h, now)
        all_findings.extend(findings)

    log.info("Quantity: %d gap(s) found across all metrics", len(all_findings))
    return all_findings


@task(name="scan_heart_rate_gaps")
def scan_heart_rate_gaps() -> list[GapFinding]:
    """Scan health_heart_rate for significant gaps."""
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
        log.info("Heart rate: only %d points in lookback window — skipping", len(rows))
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
    log.info("Heart rate: %d gap(s) found", len(findings))
    return findings


@task(name="scan_sleep_gaps")
def scan_sleep_gaps() -> list[GapFinding]:
    """
    Detect calendar nights with no sleep data at all.

    Sleep records are per-stage (many rows per night), so we aggregate to
    night-dates and look for missing nights in the continuous range.  Tonight
    is never flagged — sleep data for the current calendar day may not have
    been uploaded yet.

    A "night" is defined as the calendar date of the start_ts. This means a
    session starting at 23:00 on the 15th and ending at 06:30 on the 16th
    counts as the night of the 15th.
    """
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

    # Walk every calendar night in the recorded range and flag missing ones.
    # We intentionally do not flag trailing missing nights (after last recorded)
    # unless they were expected: this is conservative because sleep data often
    # arrives the morning after the night.
    gap_start: date | None = None
    night = first_night
    while night <= last_night:
        if night not in recorded_nights and night < today:
            if gap_start is None:
                gap_start = night
        else:
            if gap_start is not None:
                # Close this run of missing nights
                gap_start_dt = datetime(gap_start.year, gap_start.month, gap_start.day, tzinfo=timezone.utc)
                gap_end_dt = datetime(night.year, night.month, night.day, tzinfo=timezone.utc)
                gap_h = (gap_end_dt - gap_start_dt).total_seconds() / 3600
                nights_missing = (night - gap_start).days
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

@flow(name="Detect Health Gaps")
def check_health_gaps_flow() -> None:
    """
    Weekly health gap audit.  Runs three independent scans, aggregates findings,
    and sends a plain-text email report.  Silent (no email) if no gaps are found.
    """
    log = get_run_logger()

    quantity_findings = scan_quantity_gaps()
    hr_findings = scan_heart_rate_gaps()
    sleep_findings = scan_sleep_gaps()

    all_findings: list[GapFinding] = quantity_findings + hr_findings + sleep_findings
    all_findings.sort(key=lambda f: f.gap_hours, reverse=True)

    if not all_findings:
        log.info("No health gaps detected — no email sent")
        return

    confirmed = [f for f in all_findings if not f.tentative]
    tentative = [f for f in all_findings if f.tentative]

    # ---- build report ----

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