"""
database/journal/table.py
~~~~~~~~~~~~~~~~~~~~~~~~~
journal_entries — TravelNet's own store of journal entries.

Quick captures (iOS Shortcut), retro/catch-up entries and imported Apple
Journal entries all land here, so Trevor can sync from one table instead of
parsing exported zips.

Provenance is recorded from day one so Trevor never presents a retrospective or
LLM-drafted entry as a contemporaneous first-person memory:
    contemporaneous        written at (or close to) the time
    retro                  written later about an earlier day (lag_days says how much later)
    drafted_from_telemetry skeleton generated from data, awaiting the user's confirmation

mood_self / valence_self are the user's own 1-5 tap. HealthKit valence
(state_of_mind) is preferred downstream when a record exists in the join window.
"""

from dataclasses import dataclass
from typing import Optional

from database.base import BaseTable
from database.connection import get_conn

PROVENANCES = ("contemporaneous", "retro", "drafted_from_telemetry")
SOURCES = ("quick", "full", "import", "retro", "drafted")


@dataclass
class JournalEntryRecord:
    id: str
    ts_utc: str                       # when it was written (UTC, Z suffix)
    local_date: str                   # calendar day of ts_utc in the writer's timezone
    text: str
    source: str = "quick"
    provenance: str = "contemporaneous"
    tz: Optional[str] = None          # IANA name at write time
    utc_offset: Optional[str] = None  # e.g. "+1100"
    covers_start: Optional[str] = None  # local dates the entry is about
    covers_end: Optional[str] = None
    lag_days: Optional[int] = None    # local_date - covers_end, retro only
    rating: Optional[int] = None      # 1-10
    energy: Optional[int] = None      # 1-5
    tags: Optional[str] = None        # comma-separated
    mood_self: Optional[int] = None   # 1-5 tap
    valence_self: Optional[float] = None  # mood_self mapped to -1..1
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    place_id: Optional[int] = None
    enrichment_json: Optional[str] = None
    import_key: Optional[str] = None  # e.g. Apple Journal filename, for idempotent import


class JournalTable(BaseTable[JournalEntryRecord]):

    def init(self) -> None:
        with get_conn() as conn:
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS journal_entries (
                    id              TEXT PRIMARY KEY,
                    ts_utc          TEXT NOT NULL,
                    local_date      TEXT NOT NULL,
                    tz              TEXT,
                    utc_offset      TEXT,
                    covers_start    TEXT,
                    covers_end      TEXT,
                    lag_days        INTEGER,
                    source          TEXT NOT NULL
                        CHECK (source IN {SOURCES!r}),
                    provenance      TEXT NOT NULL DEFAULT 'contemporaneous'
                        CHECK (provenance IN {PROVENANCES!r}),
                    text            TEXT NOT NULL,
                    rating          INTEGER CHECK (rating BETWEEN 1 AND 10),
                    energy          INTEGER CHECK (energy BETWEEN 1 AND 5),
                    tags            TEXT,
                    mood_self       INTEGER CHECK (mood_self BETWEEN 1 AND 5),
                    valence_self    REAL CHECK (valence_self BETWEEN -1.0 AND 1.0),
                    latitude        REAL,
                    longitude       REAL,
                    place_id        INTEGER REFERENCES places(id),
                    enrichment_json TEXT,
                    import_key      TEXT UNIQUE,
                    created_at      TEXT NOT NULL
                        DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
                );
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_journal_ts ON journal_entries(ts_utc);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_journal_local_date ON journal_entries(local_date);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_journal_covers ON journal_entries(covers_start, covers_end);")

    def insert(self, record: JournalEntryRecord) -> None:
        with get_conn() as conn:
            insert_entry(conn, record)


def insert_entry(conn, r: JournalEntryRecord) -> bool:
    """INSERT OR IGNORE on a given connection. Returns True if a row was added."""
    cur = conn.execute("""
        INSERT OR IGNORE INTO journal_entries (
            id, ts_utc, local_date, tz, utc_offset, covers_start, covers_end, lag_days,
            source, provenance, text, rating, energy, tags, mood_self, valence_self,
            latitude, longitude, place_id, enrichment_json, import_key
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        r.id, r.ts_utc, r.local_date, r.tz, r.utc_offset, r.covers_start, r.covers_end,
        r.lag_days, r.source, r.provenance, r.text, r.rating, r.energy, r.tags,
        r.mood_self, r.valence_self, r.latitude, r.longitude, r.place_id,
        r.enrichment_json, r.import_key,
    ))
    return cur.rowcount > 0


table = JournalTable()
