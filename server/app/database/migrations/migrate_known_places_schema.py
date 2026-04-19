"""
migrate_known_places_schema.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Schema migration for the known_places / place_visits tables:

  1. Add  known_places.place_id INTEGER REFERENCES places(id)
  2. Backfill known_places.place_id for existing rows by snapping their
     coordinates to the nearest 0.001° grid cell — the same logic used by
     get_place_id() in database/location/geocoding.py.
  3. Rename  place_visits.place_id → place_visits.known_place_id
     (SQLite does not support ALTER COLUMN, so this requires a table rebuild.)
  4. Recreate the affected indexes.

Run from inside the Docker container (or any environment with access to travel.db):

    python migrate_known_places_schema.py [--db /path/to/travel.db] [--dry-run]

The script is idempotent: each step checks column existence before acting so
re-running on an already-migrated database is safe.

SQLite does not support ALTER COLUMN, so the place_visits rename uses the
standard 12-step approach (with PRAGMA foreign_keys = OFF during the rebuild).
ALTER TABLE ... ADD COLUMN is used for the known_places addition because
SQLite has supported it since 3.1.3.

All three steps run inside one transaction so the migration is atomic.
PRAGMA foreign_keys is set outside the transaction as SQLite requires.
"""

import argparse
import sqlite3


def col_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r[1] == column for r in rows)


def migrate_known_places_add_place_id(conn: sqlite3.Connection, dry_run: bool) -> None:
    """Add place_id INTEGER REFERENCES places(id) to known_places."""
    if col_exists(conn, "known_places", "place_id"):
        print("  known_places.place_id already exists — skipping")
        return
    print("  known_places: adding place_id INTEGER REFERENCES places(id)")
    if not dry_run:
        conn.execute(
            "ALTER TABLE known_places ADD COLUMN place_id INTEGER REFERENCES places(id)"
        )


def backfill_known_places_place_id(conn: sqlite3.Connection, dry_run: bool) -> None:
    """Backfill known_places.place_id using the same 0.001° snapping as get_place_id()."""
    if not col_exists(conn, "known_places", "place_id"):
        # Column was not yet added (dry-run skipped step 1) — report what would happen
        total = conn.execute("SELECT COUNT(*) FROM known_places").fetchone()[0]
        print(f"  known_places.place_id backfill: would update {total} row(s)")
        return
    count = conn.execute(
        "SELECT COUNT(*) FROM known_places WHERE place_id IS NULL"
    ).fetchone()[0]
    if count == 0:
        print("  known_places.place_id backfill: nothing to do")
        return
    print(f"  known_places.place_id backfill: {count} row(s) to update")
    if dry_run:
        return
    # Upsert snapped grid cells into places — identical to get_place_id()
    conn.execute("""
        INSERT OR IGNORE INTO places (lat_snap, lon_snap)
        SELECT ROUND(latitude, 3), ROUND(longitude, 3)
        FROM known_places
        WHERE place_id IS NULL
    """)
    conn.execute("""
        UPDATE known_places
        SET place_id = (
            SELECT id FROM places
            WHERE lat_snap = ROUND(known_places.latitude, 3)
              AND lon_snap = ROUND(known_places.longitude, 3)
        )
        WHERE place_id IS NULL
    """)


def migrate_place_visits_rename_place_id(conn: sqlite3.Connection, dry_run: bool) -> None:
    """Rename place_visits.place_id → place_visits.known_place_id via table rebuild."""
    if col_exists(conn, "place_visits", "known_place_id"):
        print("  place_visits.known_place_id already exists — skipping")
        return
    print("  place_visits: renaming place_id → known_place_id (table rebuild)")
    if dry_run:
        return
    conn.execute("""
        CREATE TABLE place_visits_new (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            known_place_id INTEGER NOT NULL REFERENCES known_places(id),
            arrived_at     TEXT NOT NULL,
            departed_at    TEXT,
            duration_mins  INTEGER
        )
    """)
    conn.execute("""
        INSERT INTO place_visits_new (id, known_place_id, arrived_at, departed_at, duration_mins)
        SELECT id, place_id, arrived_at, departed_at, duration_mins
        FROM place_visits
    """)
    conn.execute("DROP TABLE place_visits")
    conn.execute("ALTER TABLE place_visits_new RENAME TO place_visits")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_place_visits_known_place_id"
        " ON place_visits(known_place_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_place_visits_arrived"
        " ON place_visits(arrived_at)"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Add known_places.place_id and rename place_visits.place_id "
            "→ known_place_id"
        )
    )
    parser.add_argument("--db", default="/app/data/travel.db", help="Path to travel.db")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would be done without writing any changes",
    )
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    try:
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute("BEGIN")

        migrate_known_places_add_place_id(conn, args.dry_run)
        backfill_known_places_place_id(conn, args.dry_run)
        migrate_place_visits_rename_place_id(conn, args.dry_run)

        if args.dry_run:
            conn.execute("ROLLBACK")
            print("Dry run complete — no changes written")
        else:
            conn.execute("COMMIT")
            print("Migration complete")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.execute("PRAGMA foreign_keys = ON")
        if not args.dry_run:
            issues = conn.execute("PRAGMA foreign_key_check").fetchall()
            if issues:
                print(f"WARNING: foreign_key_check found {len(issues)} issue(s):")
                for row in issues:
                    print(f"  {dict(row)}")
            else:
                print("foreign_key_check: OK")
        conn.close()


if __name__ == "__main__":
    main()
