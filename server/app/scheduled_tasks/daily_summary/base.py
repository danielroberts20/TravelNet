"""
scheduled_tasks/daily_summary/base.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Domain registry for daily_summary.

Each domain (health, location, pi, transactions, weather) is a `Domain`
spec describing:
  - which daily_summary columns it owns
  - which completeness flag it sets
  - when a local date should be considered "closed" for that domain
  - how to compute its columns for a given date

The master flow uses this registry to orchestrate subflows. Each domain
subflow uses its own spec to upsert only its columns, leaving other
columns untouched.

This means adding a new domain is one file + one entry in DOMAINS —
no column lists to maintain in parallel anywhere else.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone as dt_timezone
from typing import Callable
from zoneinfo import ZoneInfo

import sqlite3

from database.connection import get_conn


# ---------------------------------------------------------------------------
# Completeness age predicates
# ---------------------------------------------------------------------------

def _age_days(local_date: str) -> int:
    """Days between today (UTC) and `local_date` (YYYY-MM-DD)."""
    today = datetime.now(dt_timezone.utc).date()
    d = datetime.strptime(local_date, "%Y-%m-%d").date()
    return (today - d).days


def closed_after(days: int) -> Callable[[str], bool]:
    """Return a predicate: `local_date` is closed if it's at least `days` old."""
    return lambda local_date: _age_days(local_date) >= days


def never_auto_close(local_date: str) -> bool:
    """
    Use for domains where completeness is determined by an external event
    (e.g. monthly upload) rather than by calendar age. The domain's own
    flow is responsible for setting the flag explicitly.
    """
    return False


# ---------------------------------------------------------------------------
# Domain spec
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Domain:
    name:                   str
    columns:                frozenset[str]
    completeness_flag:      str
    compute_fn:             Callable[[sqlite3.Connection, dict], dict]
    completeness_predicate: Callable[[str], bool]

    def is_closed(self, local_date: str) -> bool:
        return self.completeness_predicate(local_date)

    def upsert_for_date(self, local_date: str) -> dict:
        """
        Compute this domain's columns for the given date, then upsert
        them to daily_summary. Returns the data dict (including the
        completeness flag) for logging/testing.
        """
        with get_conn() as conn:
            ctx  = get_date_context(conn, local_date)
            data = self.compute_fn(conn, ctx)
            data[self.completeness_flag] = 1 if self.is_closed(local_date) else 0

            _upsert_domain_columns(conn, local_date, ctx, self, data)
        return data


# ---------------------------------------------------------------------------
# Shared utilities
# ---------------------------------------------------------------------------

def get_date_context(conn, local_date: str) -> dict:
    """
    Resolve the UTC window for a local date using transition_timezone.
    Returned dict is merged into the upsert — it populates date context
    columns that belong to no single domain.
    """
    row = conn.execute("""
        SELECT to_tz, to_offset FROM transition_timezone
        WHERE transitioned_at <= ?
        ORDER BY transitioned_at DESC LIMIT 1
    """, (f"{local_date}T23:59:59Z",)).fetchone()

    if row:
        tz_name    = row["to_tz"]
        utc_offset = row["to_offset"]
    else:
        # No transition recorded yet — infer from system timezone.
        # Pre-departure the Pi is in London, so this is correct.
        # Post-departure, transition_timezone will always have a row.
        import zoneinfo
        system_tz = datetime.now().astimezone().tzinfo
        # Use a known default rather than relying on system tzinfo name
        tz_name    = "Europe/London"
        utc_offset = datetime.now(ZoneInfo("Europe/London")).strftime("%z")
        utc_offset = f"{utc_offset[:3]}:{utc_offset[3:]}"
    
    tz = ZoneInfo(tz_name)

    local_midnight = datetime.strptime(local_date, "%Y-%m-%d").replace(tzinfo=tz)
    next_midnight  = local_midnight + timedelta(days=1)

    return {
        "date":       local_date,
        "timezone":   tz_name,
        "utc_offset": utc_offset,
        "utc_start":  local_midnight.astimezone(dt_timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "utc_end":    next_midnight .astimezone(dt_timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


# Date context columns — always (re)written by any subflow to keep them
# consistent, since they're cheap to derive and don't belong to any domain.
DATE_CONTEXT_COLUMNS = frozenset({
    "date", "timezone", "utc_offset", "utc_start", "utc_end",
})


def _upsert_domain_columns(
    conn: sqlite3.Connection,
    local_date: str,
    ctx: dict,
    domain: Domain,
    data: dict,
) -> None:
    """
    Insert-or-update the row for `local_date`, writing only the columns
    in `domain.columns ∪ DATE_CONTEXT_COLUMNS ∪ {completeness_flag}`.
    All other columns keep their existing values (or NULL if this is a
    new row).

    Uses the INSERT ... ON CONFLICT(date) DO UPDATE SET ... pattern.
    """
    write_cols = (
        DATE_CONTEXT_COLUMNS
        | domain.columns
        | {domain.completeness_flag}
    )

    # Merge ctx + data; only columns in write_cols are touched
    values = {**ctx, **data}
    cols   = [c for c in write_cols if c in values]
    placeholders = ", ".join("?" for _ in cols)
    insert_cols  = ", ".join(cols)

    # ON CONFLICT UPDATE clause — excluded.<col> refers to the would-have-
    # been-inserted value
    update_cols = [c for c in cols if c != "date"]
    update_set  = ", ".join(f"{c} = excluded.{c}" for c in update_cols)
    update_set += ", computed_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')"

    conn.execute(
        f"""
        INSERT INTO daily_summary ({insert_cols})
        VALUES ({placeholders})
        ON CONFLICT(date) DO UPDATE SET {update_set}
        """,
        tuple(values[c] for c in cols),
    )