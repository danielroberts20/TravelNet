"""
database/journal/timing.py
~~~~~~~~~~~~~~~~~~~~~~~~~~
When to nudge: a small statistical model of when the user is typically quiet.

Signals
  quietness   mean steps per local (weekday, hour), only over hours in which the
              phone was demonstrably alive (it reported a location), so a dead
              battery is not mistaken for a quiet evening. Recent weeks weigh more.
  writing     the (weekday, hour) at which the user has actually written entries
              (contemporaneous only), blended in once there are enough of them.
  sleep       typical wake and sleep-onset time, so a nudge never lands within
              `wake_buffer` of waking or `sleep_buffer` of going to sleep.

A slot's score is its normalised quietness (blended with the writing profile once
`learn_min_entries` entries exist). With no data at all every slot ties and the
tie-break picks the hour closest to `fallback_hour`, so a cold start behaves like
a fixed evening reminder.

Pure functions take plain data; the load_* helpers read the database.
"""

from bisect import bisect_right
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

_TS = "%Y-%m-%dT%H:%M:%SZ"
MIN_OBS = 2.0           # weighted observations needed to trust a weekday-specific hour
WEEK_DECAY = 0.85       # weight multiplier per week of age
FALLBACK_PULL = 0.1     # max score penalty for being far from the fallback hour


# ---------------------------------------------------------------------------
# Timezones
# ---------------------------------------------------------------------------

def load_transitions(conn) -> list[tuple[datetime, ZoneInfo]]:
    rows = conn.execute(
        "SELECT transitioned_at, to_tz FROM transition_timezone ORDER BY transitioned_at"
    ).fetchall()
    out = []
    for r in rows:
        try:
            out.append((datetime.strptime(r["transitioned_at"], _TS).replace(tzinfo=timezone.utc),
                        ZoneInfo(r["to_tz"])))
        except Exception:
            continue
    return out


def tz_at(transitions, when_utc: datetime) -> ZoneInfo:
    """The timezone in force at an instant (the earliest known one before any transition)."""
    if not transitions:
        return ZoneInfo("UTC")
    i = bisect_right([t for t, _ in transitions], when_utc) - 1
    return transitions[max(i, 0)][1]


def to_local(transitions, when_utc: datetime) -> datetime:
    return when_utc.astimezone(tz_at(transitions, when_utc))


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

def load_activity(conn, since_utc: datetime, until_utc: datetime, transitions):
    """Return (steps, alive): steps[(date, hour)] -> steps; alive = set of (date, hour)."""
    steps: dict = {}
    rows = conn.execute("""
        WITH deduped AS (
            SELECT timestamp, value, ROW_NUMBER() OVER (
                PARTITION BY timestamp
                ORDER BY CASE WHEN source LIKE '%Watch%' THEN 0 WHEN source LIKE '%iPhone%' THEN 1 ELSE 2 END
            ) AS rn
            FROM health_quantity
            WHERE metric = 'Step Count' AND timestamp >= ? AND timestamp < ?
        )
        SELECT timestamp, value FROM deduped WHERE rn = 1
    """, (since_utc.strftime(_TS), until_utc.strftime(_TS))).fetchall()
    for r in rows:
        t = datetime.strptime(r["timestamp"], _TS).replace(tzinfo=timezone.utc)
        loc = to_local(transitions, t)
        key = (loc.date(), loc.hour)
        steps[key] = steps.get(key, 0.0) + (r["value"] or 0.0)

    alive = set()
    for r in conn.execute(
        "SELECT DISTINCT substr(timestamp, 1, 13) AS h FROM location_unified WHERE timestamp >= ? AND timestamp < ?",
        (since_utc.strftime(_TS), until_utc.strftime(_TS)),
    ).fetchall():
        t = datetime.strptime(r["h"] + ":30:00Z", _TS).replace(tzinfo=timezone.utc)   # mid-hour avoids edge ties
        loc = to_local(transitions, t)
        alive.add((loc.date(), loc.hour))
    return steps, alive


def load_writing_times(conn, since_utc: datetime, transitions) -> list[datetime]:
    """Local times at which contemporaneous entries were written."""
    rows = conn.execute(
        "SELECT ts_utc FROM journal_entries WHERE provenance = 'contemporaneous' AND ts_utc >= ?",
        (since_utc.strftime(_TS),),
    ).fetchall()
    return [to_local(transitions, datetime.strptime(r["ts_utc"], _TS).replace(tzinfo=timezone.utc)) for r in rows]


@dataclass
class Bounds:
    wake_min: int      # typical wake, minutes since local midnight
    onset_min: int     # typical sleep onset; > 1440 means after midnight


FALLBACK_BOUNDS = Bounds(wake_min=8 * 60, onset_min=23 * 60 + 30)


def _weighted_median(values, weights):
    paired = sorted(zip(values, weights))
    total, cum = sum(weights), 0.0
    for v, w in paired:
        cum += w
        if cum >= total / 2:
            return v
    return paired[-1][0]


def load_sleep_bounds(conn, now_utc: datetime, transitions, lookback_days: int = 14, min_nights: int = 3) -> Bounds:
    """Weighted-median wake and onset over recent nights (noon-to-noon attribution)."""
    rows = conn.execute("""
        SELECT start_ts, end_ts FROM health_sleep
        WHERE stage NOT IN ('In Bed', 'Awake') AND start_ts >= ? ORDER BY start_ts
    """, ((now_utc - timedelta(days=lookback_days + 2)).strftime(_TS),)).fetchall()
    nights: dict = {}
    for r in rows:
        s = datetime.strptime(r["start_ts"], _TS).replace(tzinfo=timezone.utc)
        e = datetime.strptime(r["end_ts"], _TS).replace(tzinfo=timezone.utc)
        sl = to_local(transitions, s)
        key = (sl - timedelta(hours=12)).date()
        first, last = nights.get(key, (s, e))
        nights[key] = (min(first, s), max(last, e))
    ordered = [nights[k] for k in sorted(nights)][-lookback_days:]
    if len(ordered) < min_nights:
        return FALLBACK_BOUNDS
    onsets, wakes = [], []
    for s, e in ordered:
        sl, el = to_local(transitions, s), to_local(transitions, e)
        o = sl.hour * 60 + sl.minute
        onsets.append(o + 1440 if o < 12 * 60 else o)
        wakes.append(el.hour * 60 + el.minute)
    w = list(range(1, len(ordered) + 1))
    return Bounds(int(_weighted_median(wakes, w)), int(_weighted_median(onsets, w)))


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------

def _weeks_ago(d: date, today: date) -> int:
    return max(0, (today - d).days // 7)


def quiet_profile(steps, alive, today: date) -> dict:
    """{(weekday, hour): (mean_steps, weight)} over alive hours, recency-weighted."""
    acc: dict = {}
    for (d, h) in alive:
        w = WEEK_DECAY ** _weeks_ago(d, today)
        tot, wt = acc.get((d.weekday(), h), (0.0, 0.0))
        acc[(d.weekday(), h)] = (tot + w * steps.get((d, h), 0.0), wt + w)
    return {k: (t / wt, wt) for k, (t, wt) in acc.items() if wt > 0}


def writing_profile(times: list[datetime], today: date) -> dict:
    prof: dict = {}
    for t in times:
        k = (t.weekday(), t.hour)
        prof[k] = prof.get(k, 0.0) + WEEK_DECAY ** _weeks_ago(t.date(), today)
    return prof


def _mean_for(profile: dict, weekday: int, hour: int) -> Optional[float]:
    """Weekday-specific mean when well observed, else the all-weekday mean for that hour."""
    exact = profile.get((weekday, hour))
    if exact and exact[1] >= MIN_OBS:
        return exact[0]
    pooled = [(m, w) for (wd, h), (m, w) in profile.items() if h == hour]
    wt = sum(w for _, w in pooled)
    return sum(m * w for m, w in pooled) / wt if wt >= MIN_OBS else None


def candidate_hours(bounds: Bounds, wake_buffer_min: int, sleep_buffer_min: int) -> list[int]:
    """Whole hours whose start lies in [wake + wake_buffer, onset - sleep_buffer], same calendar day."""
    first = -(-(bounds.wake_min + wake_buffer_min) // 60)           # ceil
    last = min(23, (bounds.onset_min - sleep_buffer_min) // 60)
    return list(range(max(first, 0), last + 1))


def score_hours(weekday: int, hours: list[int], quiet: dict, writing: dict,
                n_entries: int, learn_min_entries: int, fallback_hour: int) -> dict:
    """{hour: score} (higher = better time to nudge) for one weekday."""
    means = {h: _mean_for(quiet, weekday, h) for h in hours}
    known = [v for v in means.values() if v is not None]
    lo, hi = (min(known), max(known)) if known else (0.0, 0.0)

    def quiet_score(h):
        v = means[h]
        if v is None or hi == lo:
            return 0.5
        return 1.0 - (v - lo) / (hi - lo)

    use_writing = n_entries >= learn_min_entries and writing
    wvals = {h: writing.get((weekday, h), 0.0) for h in hours}
    wmax = max(wvals.values()) if wvals else 0.0

    scores = {}
    for h in hours:
        s = quiet_score(h)
        if use_writing and wmax > 0:
            s = 0.5 * s + 0.5 * (wvals[h] / wmax)
        # Soft prior toward the fallback evening hour (max 0.1 penalty): decides cold starts and ties, and stops
        # the pure-quietness signal drifting to the last hour before sleep, when steps are lowest.
        scores[h] = s - FALLBACK_PULL * abs(h - fallback_hour) / 12
    return scores


def pick_today(now_local: datetime, bounds: Bounds, quiet: dict, writing: dict, n_entries: int, *,
               wake_buffer_min: int, sleep_buffer_min: int, learn_min_entries: int,
               fallback_hour: int, min_lead_min: int = 10) -> Optional[datetime]:
    """Best slot later today, or None if the window has closed."""
    hours = candidate_hours(bounds, wake_buffer_min, sleep_buffer_min)
    earliest = now_local + timedelta(minutes=min_lead_min)
    scores = score_hours(now_local.weekday(), hours, quiet, writing, n_entries, learn_min_entries, fallback_hour)
    best = None
    for h, s in scores.items():
        slot = now_local.replace(hour=h, minute=0, second=0, microsecond=0)
        if slot >= earliest and (best is None or s > best[0]):
            best = (s, slot)
    return best[1] if best else None


def pick_weekly(now_local: datetime, earliest_date: date, bounds: Bounds, quiet: dict, writing: dict,
                n_entries: int, *, days_ahead: int = 7, wake_buffer_min: int, sleep_buffer_min: int,
                learn_min_entries: int, fallback_hour: int, min_lead_min: int = 10) -> Optional[datetime]:
    """Best slot across the next `days_ahead` days (never before earliest_date); mildly prefers sooner."""
    hours = candidate_hours(bounds, wake_buffer_min, sleep_buffer_min)
    earliest = now_local + timedelta(minutes=min_lead_min)
    best = None
    for off in range(days_ahead):
        day = (now_local + timedelta(days=off)).date()
        if day < earliest_date:
            continue
        scores = score_hours(day.weekday(), hours, quiet, writing, n_entries, learn_min_entries, fallback_hour)
        for h, s in scores.items():
            slot = datetime(day.year, day.month, day.day, h, tzinfo=now_local.tzinfo)
            if slot < earliest:
                continue
            s -= 0.02 * off
            if best is None or s > best[0]:
                best = (s, slot)
    return best[1] if best else None


def is_transit_day(conn, utc_start: str, utc_end: str) -> bool:
    """True if a flight or a timezone change falls within the local day's UTC window."""
    if conn.execute(
        "SELECT 1 FROM flights WHERE departed_at < ? AND arrived_at >= ? LIMIT 1", (utc_end, utc_start)
    ).fetchone():
        return True
    return conn.execute(
        "SELECT 1 FROM transition_timezone WHERE transitioned_at >= ? AND transitioned_at < ? LIMIT 1",
        (utc_start, utc_end),
    ).fetchone() is not None
