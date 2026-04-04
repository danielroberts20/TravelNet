from database.connection import get_conn


def init() -> None:

    with get_conn() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS trigger_log (
            id          INTEGER PRIMARY KEY,
            trigger     TEXT NOT NULL,
            fired_at    TEXT NOT NULL,
            place_id    INTEGER REFERENCES places(id),
            payload     TEXT
        );""")
        