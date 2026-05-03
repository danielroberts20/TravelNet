"""
database/transition/country/table.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Schema and helpers for the country_transitions table.
 
Tracks when the user crosses into a new country, derived from
location_unified joined to places. Populated by the scheduled task
detect_country_transitions.py.
"""
 
from dataclasses import dataclass
 
from database.base import BaseTable
from database.connection import get_conn
 
 
@dataclass
class WatchdogHeartbeatRecord:
    timestamp: str          
    internet_ok: int|bool
    tailscale_ok: int|bool      
    api_ok: int|bool
    prefect_ok: int|bool
    consecutive_failures: int = 0
 
 
class WatchdogHeartbeatTable(BaseTable[WatchdogHeartbeatRecord]):
 
    def init(self) -> None:
        """Create country_transitions and its indexes if they don't exist."""
        with get_conn() as conn:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS watchdog_heartbeat (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                received_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
                timestamp    TEXT NOT NULL,
                internet_ok  INTEGER NOT NULL,
                tailscale_ok INTEGER NOT NULL,
                api_ok       INTEGER NOT NULL,
                prefect_ok   INTEGER NOT NULL,
                consecutive_failures INTEGER NOT NULL DEFAULT 0
            )""")

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_watchdog_received_at
                ON watchdog_heartbeat(received_at)
            """)
 
    def insert(self, record: WatchdogHeartbeatRecord) -> bool:
        """Insert a watchdog heartbeat row.
 
        Uses INSERT OR IGNORE — safe to re-run. Returns True if a new row
        was inserted, False if it already existed.
        """
        with get_conn() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO watchdog_heartbeat
                    (timestamp, internet_ok, tailscale_ok, api_ok, prefect_ok, consecutive_failures)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                record.timestamp,
                int(record.internet_ok),
                int(record.tailscale_ok),
                int(record.api_ok),
                int(record.prefect_ok),
                record.consecutive_failures
            ))

            conn.execute("""
                DELETE FROM watchdog_heartbeat
                WHERE id NOT IN (
                    SELECT id FROM watchdog_heartbeat
                    ORDER BY received_at DESC
                    LIMIT 1440
                )
            """)
            return conn.execute("SELECT changes()").fetchone()[0] > 0
 
 
table = WatchdogHeartbeatTable()