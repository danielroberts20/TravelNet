"""
health_gaps/detect.py
~~~~~~~~~~~~~~~~~~~~~
Pure gap detection: timestamps in, gaps and clustered candidate events out.
No database or logging access, so it is trivially testable.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from health_gaps.config import (
    HEART_RATE,
    SLEEP,
    config_for,
    group_key_for,
)

ISO = "%Y-%m-%dT%H:%M:%SZ"


def fmt(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime(ISO)


def parse(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


@dataclass(frozen=True)
class Gap:
    metric: str
    start: datetime
    end: Optional[datetime]      # None => ongoing ("trailing")
    points_after: int = 0

    @property
    def ongoing(self) -> bool:
        return self.end is None


@dataclass
class Candidate:
    """A cluster of overlapping gaps from the same group."""
    group_key: str
    gaps: list[Gap] = field(default_factory=list)

    @property
    def start(self) -> datetime:
        return min(g.start for g in self.gaps)

    @property
    def end(self) -> Optional[datetime]:
        """None if any member gap is still ongoing."""
        if any(g.ongoing for g in self.gaps):
            return None
        return max(g.end for g in self.gaps)  # type: ignore[type-var]

    @property
    def points_after(self) -> int:
        return max(g.points_after for g in self.gaps)

    @property
    def metrics(self) -> dict[str, tuple[datetime, Optional[datetime]]]:
        return {g.metric: (g.start, g.end) for g in self.gaps}


# ---------------------------------------------------------------------------
# Continuous / activity metrics
# ---------------------------------------------------------------------------

def detect_metric_gaps(
    metric: str,
    timestamps: list[datetime],
    now: datetime,
    last_seen: Optional[datetime] = None,
) -> list[Gap]:
    """Gaps >= the metric's absolute threshold.

    `timestamps` are the points inside the scan window; `last_seen` is the
    metric's most recent timestamp overall (used for the trailing gap when the
    window holds no points). A metric that has never reported is ignored.
    """
    cfg = config_for(metric)
    if cfg is None or metric == SLEEP:
        return []
    threshold = timedelta(hours=cfg.threshold_h)
    ts = sorted(timestamps)
    gaps: list[Gap] = []

    for i in range(1, len(ts)):
        if ts[i] - ts[i - 1] >= threshold:
            gaps.append(Gap(metric, ts[i - 1], ts[i], points_after=len(ts) - i))

    newest = ts[-1] if ts else last_seen
    if last_seen and (newest is None or last_seen > newest):
        newest = last_seen
    if newest is not None and now - newest >= threshold:
        gaps.append(Gap(metric, newest, None, points_after=0))
    return gaps


# ---------------------------------------------------------------------------
# Sleep (nightly)
# ---------------------------------------------------------------------------

def detect_sleep_gaps(
    night_rows: dict[date, int],
    today: date,
    min_nights: int,
) -> list[Gap]:
    """Runs of >= `min_nights` consecutive missing nights.

    `night_rows` maps the UTC date of each recorded sleep row to its row count.
    A single missed night is never a gap. Today is excluded (still being
    recorded). A run that extends to yesterday is an ongoing gap.
    """
    if not night_rows:
        return []
    gaps: list[Gap] = []
    first, last = min(night_rows), max(night_rows)
    run_start: Optional[date] = None
    d = first
    while d <= last:
        if d not in night_rows:
            if run_start is None:
                run_start = d
        elif run_start is not None:
            gaps.extend(_sleep_run(night_rows, run_start, d, min_nights))
            run_start = None
        d += timedelta(days=1)

    # trailing: from the day after the last recorded night up to yesterday
    trailing_start = last + timedelta(days=1)
    missing = (today - trailing_start).days   # nights trailing_start .. today-1
    if missing >= min_nights:
        gaps.append(Gap(SLEEP, _midnight(trailing_start), None, 0))
    return gaps


def _sleep_run(night_rows, run_start: date, run_end: date, min_nights: int) -> list[Gap]:
    if (run_end - run_start).days < min_nights:
        return []
    after = sum(c for d, c in night_rows.items() if d >= run_end)
    return [Gap(SLEEP, _midnight(run_start), _midnight(run_end), points_after=after)]


def _midnight(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Clustering
# ---------------------------------------------------------------------------

def cluster_gaps(gaps: list[Gap], now: datetime) -> list[Candidate]:
    """Merge gaps of the same group whose time windows overlap into one event."""
    by_group: dict[str, list[Gap]] = {}
    for g in gaps:
        by_group.setdefault(group_key_for(g.metric), []).append(g)

    out: list[Candidate] = []
    for key, members in by_group.items():
        members.sort(key=lambda g: g.start)
        current: Optional[Candidate] = None
        current_end = now
        for g in members:
            g_end = g.end or now
            if current is not None and g.start <= current_end:
                current.gaps.append(g)
                current_end = max(current_end, g_end)
            else:
                current = Candidate(key, [g])
                current_end = g_end
                out.append(current)
    out.sort(key=lambda c: c.start)
    return out
