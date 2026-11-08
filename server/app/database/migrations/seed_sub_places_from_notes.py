"""
database/migrations/seed_sub_places_from_notes.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
One-time, user-approved seed of sub_places from places whose free-text notes
used to list the spots inside them. The curated list below was reviewed and
approved row by row; nothing is parsed from notes automatically.

known_places.notes are left untouched. Safe to re-run: a sub-place is skipped
if an active one with the same name already exists under the parent. Each
parent is checked against its expected label first, so a wrong DB (or a
renumbered id) cannot attach rows to the wrong place.

    python seed_sub_places_from_notes.py --db /path/to/travel.db [--dry-run]

Requires the sub_places / sub_place_types tables (created by the app's
schema init on startup).
"""

import argparse
import sqlite3
from datetime import datetime, timezone

# parent id -> (expected label, [(sub-place name, type or None[, notes]), ...])
SEED: dict[int, tuple[str, list[tuple]]] = {
    21:  ("Main Camp", [("Rec Hall", None), ("Bunk 23", None, "Orientation"), ("Pool", None),
                        ("Rox", None), ("Waterfront Hut", None)]),
    26:  ("Walmart Queensbury", [("Discount Liquor Store", "shop")]),
    27:  ("Senior Village", [("Northwestern", None), ("Clemson", None), ("SV Pool", None),
                             ("Lodge", None), ("Lineup", None)]),
    67:  ("Imperial Beach", [("Beach", None), ("Lifeguard station", None)]),
    91:  ("Golden Gate Park", [("Blue Heron Lake", "park")]),
    92:  ("Walgreens San Francisco", [("Bus stop", None)]),
    96:  ("Green Tortoise Hostel", [("Kell’s Irish Pub", "bar")]),
    130: ("Nomads St Kilda", [("Jekyll & Hyde", "bar"), ("Woolies", "supermarket"),
                              ("Coles Acland St", "supermarket")]),
    137: ("Southbank", [("KFC", "restaurant"), ("Unclebooom pickup", None)]),
    138: ("Flinders Station", [("Plaza Ballroom", None)]),
    140: ("Balaclava", [("The Local Taphouse", "bar"), ("Big Coles", "supermarket"),
                        ("Woolies Metro", "supermarket")]),
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("BEGIN")
    added = skipped = 0
    try:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        for parent_id, (label, subs) in SEED.items():
            row = conn.execute("SELECT label FROM known_places WHERE id = ?", (parent_id,)).fetchone()
            if row is None or row["label"] != label:
                raise SystemExit(f"Parent {parent_id}: expected label {label!r}, found "
                                 f"{row['label'] if row else None!r} — aborting, nothing written")
            for name, type_name, *rest in subs:
                notes = rest[0] if rest else None
                if conn.execute(
                    "SELECT 1 FROM sub_places WHERE known_place_id = ? AND name = ? AND archived_at IS NULL",
                    (parent_id, name),
                ).fetchone():
                    skipped += 1
                    print(f"  skip   {label} / {name} (exists)")
                    continue
                type_id = None
                if type_name:
                    conn.execute("INSERT OR IGNORE INTO sub_place_types (name) VALUES (?)", (type_name,))
                    type_id = conn.execute(
                        "SELECT id FROM sub_place_types WHERE name = ?", (type_name,)
                    ).fetchone()[0]
                conn.execute(
                    "INSERT INTO sub_places (known_place_id, name, type_id, notes, created_at) VALUES (?, ?, ?, ?, ?)",
                    (parent_id, name, type_id, notes, now),
                )
                added += 1
                print(f"  add    {label} / {name} [{type_name or '-'}]")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    if args.dry_run:
        conn.execute("ROLLBACK")
        print(f"\nDry run — {added} would be added, {skipped} skipped; nothing written")
    else:
        conn.execute("COMMIT")
        print(f"\nCommitted — {added} added, {skipped} skipped")
    conn.close()


if __name__ == "__main__":
    main()
