"""
database/migrations/backfill_place_visits_superseded.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
One-time backfill to flag duplicate/overlapping place_visits rows produced by
the known-place race condition and the retroactive-scanner forward-clustering
bug (see the location-duplicates exploration report). Follows the raw-data-
immutability convention: no rows are ever deleted. Offending rows are flagged
via place_visits.superseded_by instead, and downstream readers query the
place_visits_cleaned view rather than place_visits directly.

Two passes, scoped per known_place_id:

  1. Containment: repeatedly find a visit A that fully contains another visit
     B (A.arrived_at <= B.arrived_at AND A.departed_at >= B.departed_at,
     different ids, neither already superseded) and set
     A.superseded_by = B.id. A is the artifact here — its departed_at was
     computed from a much later point (the get_last_in_radius_timestamp bug
     fixed alongside this script), so it swallows one or more legitimate
     later revisits. Repeats per known place until no more containments
     remain.

  2. Residual near-duplicates: any remaining pair at the same known_place_id
     whose arrived_at values are within 60 seconds of each other (the
     concurrent-insert race condition, now prevented by
     idx_place_visits_one_open_per_place) is printed for manual review —
     this script does NOT auto-resolve them.

This script does not touch the live database. Run it against a COPY of
travel.db, review the printed report, and only apply the same run for real
once the diff has been reviewed:

    python backfill_place_visits_superseded.py --db /path/to/copy.db [--dry-run]

The copy must already have the place_visits.superseded_by column (added by
KnownPlacesTable.init() — run the app's schema init against the copy first,
or apply the ALTER TABLE by hand).
"""

import argparse
import sqlite3


def find_containment_pair(conn: sqlite3.Connection, known_place_id: int):
    """Return one (a_id, b_id) containing pair at this known place, or None."""
    return conn.execute("""
        SELECT a.id AS a_id, b.id AS b_id
        FROM place_visits a
        JOIN place_visits b
          ON b.known_place_id = a.known_place_id
         AND b.id != a.id
        WHERE a.known_place_id = ?
          AND a.superseded_by IS NULL
          AND b.superseded_by IS NULL
          AND a.departed_at IS NOT NULL
          AND b.departed_at IS NOT NULL
          AND a.arrived_at <= b.arrived_at
          AND a.departed_at >= b.departed_at
          AND NOT (a.arrived_at = b.arrived_at AND a.departed_at = b.departed_at)
        ORDER BY a.arrived_at, b.arrived_at
        LIMIT 1
    """, (known_place_id,)).fetchone()


def supersede_containments(conn: sqlite3.Connection) -> list[tuple]:
    """Flag containing visits as superseded_by their contained visit, per known place.

    Writes are applied to `conn` unconditionally — callers wanting a dry run
    should roll back the enclosing transaction instead of skipping the writes,
    since later iterations of the containment search need to see earlier
    flags within the same run.

    Returns the list of (known_place_id, a_id, b_id) flagged, for reporting.
    """
    flagged = []
    place_ids = [r[0] for r in conn.execute("SELECT id FROM known_places").fetchall()]
    for place_id in place_ids:
        while True:
            pair = find_containment_pair(conn, place_id)
            if pair is None:
                break
            a_id, b_id = pair["a_id"], pair["b_id"]
            conn.execute(
                "UPDATE place_visits SET superseded_by = ? WHERE id = ?",
                (b_id, a_id),
            )
            flagged.append((place_id, a_id, b_id))
    return flagged


def find_residual_near_duplicates(conn: sqlite3.Connection, tolerance_seconds: int = 60):
    """Return remaining same-known-place pairs whose arrived_at is within
    tolerance_seconds of each other, for manual review (not auto-resolved)."""
    return conn.execute("""
        SELECT a.id AS a_id, a.known_place_id, a.arrived_at AS a_arrived, a.departed_at AS a_departed,
               b.id AS b_id, b.arrived_at AS b_arrived, b.departed_at AS b_departed
        FROM place_visits a
        JOIN place_visits b
          ON b.known_place_id = a.known_place_id
         AND b.id > a.id
        WHERE a.superseded_by IS NULL
          AND b.superseded_by IS NULL
          AND ABS(strftime('%s', a.arrived_at) - strftime('%s', b.arrived_at)) <= ?
        ORDER BY a.known_place_id, a.arrived_at
    """, (tolerance_seconds,)).fetchall()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Flag duplicate/overlapping place_visits rows via superseded_by"
    )
    parser.add_argument("--db", required=True, help="Path to travel.db (use a COPY, not the live file)")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Report what would be flagged without persisting changes",
    )
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    cols = [r[1] for r in conn.execute("PRAGMA table_info(place_visits)").fetchall()]
    if "superseded_by" not in cols:
        raise SystemExit(
            "place_visits.superseded_by column not found — run the app's schema "
            "init (KnownPlacesTable.init()) against this DB copy first."
        )

    conn.execute("BEGIN")
    try:
        flagged = supersede_containments(conn)
        print(f"Containment pass: {len(flagged)} visit(s) flagged as superseded")
        for place_id, a_id, b_id in flagged:
            print(f"  known_place_id={place_id}: visit {a_id} superseded_by visit {b_id} (contained)")

        residual = find_residual_near_duplicates(conn)
        if residual:
            print(f"\n{len(residual)} residual near-duplicate pair(s) within 60s — NOT auto-resolved, review manually:")
            for r in residual:
                print(
                    f"  known_place_id={r['known_place_id']}: "
                    f"visit {r['a_id']} (arrived {r['a_arrived']}, departed {r['a_departed']}) vs "
                    f"visit {r['b_id']} (arrived {r['b_arrived']}, departed {r['b_departed']})"
                )
        else:
            print("\nNo residual near-duplicate pairs found within 60s.")
    finally:
        if args.dry_run:
            conn.execute("ROLLBACK")
            print("\nDry run — no changes written")
        else:
            conn.execute("COMMIT")
            print("\nChanges committed")
        conn.close()


if __name__ == "__main__":
    main()
