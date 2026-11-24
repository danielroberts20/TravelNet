"""
database/health/gap_events/table.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Persistent state for health data gap detection.

One row per gap *event*. An event is one or more metrics that stopped
reporting over an overlapping time window (e.g. the Watch vitals going quiet
together is a single event, not one row per metric).

status
    open           gap detected but not yet trusted (ongoing, or too few points
                   have followed to know the feed is back). Never notified.
    confirmed      gap closed (or has been open for HEALTH_GAP_OPEN_ESCALATE_DAYS)
                   - data is missing. Notified once.
    resolved       the data turned up (backfill / manual export filled it).
    unrecoverable  a manual export was checked off and the data is still absent,
                   or the gap was dismissed: it is genuinely not there.

The daily check never re-notifies for an unchanged state: it emails only when
notified_status differs from status (or a critical event has not been pushed).
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Optional

from database.base import BaseTable
from database.connection import get_conn

SCHEMA = """
CREATE TABLE IF NOT EXISTS health_gap_events (
    id               INTEGER PRIMARY KEY,
    group_key        TEXT NOT NULL,
    metrics          TEXT NOT NULL,            -- JSON {metric: [start, end|null]}
    window_start     TEXT NOT NULL,
    window_end       TEXT,                     -- NULL while the gap is ongoing
    status           TEXT NOT NULL DEFAULT 'open'
                     CHECK (status IN ('open','confirmed','resolved','unrecoverable')),
    critical         INTEGER NOT NULL DEFAULT 0,
    immediate        INTEGER NOT NULL DEFAULT 1,
    first_seen       TEXT NOT NULL,
    last_seen        TEXT NOT NULL,
    notified_status  TEXT,                     -- last status we told the user about
    last_notified    TEXT,
    pushed_at        TEXT,                     -- Pushcut sent (critical events)
    exported_at      TEXT,                     -- user checked off a manual HAE export
    resolved_at      TEXT,
    note             TEXT
);
"""

INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_hge_status ON health_gap_events(status)",
    "CREATE INDEX IF NOT EXISTS idx_hge_group_start ON health_gap_events(group_key, window_start)",
)


def init_schema(conn: sqlite3.Connection) -> None:
    conn.execute(SCHEMA)
    for ddl in INDEXES:
        conn.execute(ddl)


@dataclass
class HealthGapEventRecord:
    group_key: str
    metrics: str
    window_start: str
    first_seen: str
    last_seen: str


class HealthGapEventsTable(BaseTable[HealthGapEventRecord]):

    def init(self) -> None:
        with get_conn() as conn:
            init_schema(conn)

    def insert(self, record: HealthGapEventRecord) -> None:
        with get_conn() as conn:
            conn.execute(
                """
                INSERT INTO health_gap_events
                    (group_key, metrics, window_start, first_seen, last_seen)
                VALUES (?, ?, ?, ?, ?)
                """,
                (record.group_key, record.metrics, record.window_start,
                 record.first_seen, record.last_seen),
            )


table = HealthGapEventsTable()
