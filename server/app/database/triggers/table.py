from database.util import get_conn


def init() -> None:

    with get_conn() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS trigger_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            trigger     TEXT NOT NULL,        -- 'location_change', 'spending_spike', etc.
            fired_at    TEXT NOT NULL,        -- ISO 8601 UTC
            payload     TEXT                  -- JSON blob: the value/context that triggered it
        );""")
        