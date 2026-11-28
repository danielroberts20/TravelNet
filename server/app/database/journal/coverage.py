"""
database/journal/coverage.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Which days count as "journalled" for the purpose of reminders.

This is deliberately stricter than daily_summary.journal_written (which counts
any entry that covers a day, however late): a reminder should not be silenced
because you wrote something vague about a day long gone.

A day counts when an entry covers it and it is one of:
  - a contemporaneous entry,
  - a multi-day entry (the weekly catch-up exists precisely to clear a backlog), or
  - a single-day retro entry written within `retro_count_days` of that day.
Unconfirmed `drafted_from_telemetry` entries never count.
"""

from datetime import date, timedelta


def counted_days(conn, since: date, until: date, retro_count_days: int) -> set[date]:
    rows = conn.execute("""
        SELECT provenance, local_date, covers_start, covers_end FROM journal_entries
        WHERE provenance IN ('contemporaneous', 'retro')
          AND COALESCE(covers_end, local_date) >= ? AND COALESCE(covers_start, local_date) <= ?
    """, (since.isoformat(), until.isoformat())).fetchall()

    days: set[date] = set()
    for r in rows:
        start = date.fromisoformat(r["covers_start"] or r["local_date"])
        end = date.fromisoformat(r["covers_end"] or r["local_date"])
        written = date.fromisoformat(r["local_date"])
        multi = end > start
        d = max(start, since)
        while d <= min(end, until):
            if r["provenance"] == "contemporaneous" or multi or (written - d).days <= retro_count_days:
                days.add(d)
            d += timedelta(days=1)
    return days


def missed_streak(conn, today: date, retro_count_days: int, cap: int = 120) -> int:
    """Consecutive uncovered days ending yesterday (today is not yet over, so is excluded)."""
    yesterday = today - timedelta(days=1)
    covered = counted_days(conn, yesterday - timedelta(days=cap), yesterday, retro_count_days)
    streak, d = 0, yesterday
    while streak < cap and d not in covered:
        streak += 1
        d -= timedelta(days=1)
    return streak


def covered_today(conn, today: date, retro_count_days: int) -> bool:
    return today in counted_days(conn, today, today, retro_count_days)


def entry_saved_since(conn, created_at_utc: str) -> bool:
    """Any real (non-draft) entry stored at or after the given UTC timestamp."""
    return conn.execute(
        "SELECT 1 FROM journal_entries WHERE provenance != 'drafted_from_telemetry' AND created_at >= ? LIMIT 1",
        (created_at_utc,),
    ).fetchone() is not None
