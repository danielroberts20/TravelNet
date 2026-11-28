"""
database/journal/mood.py
~~~~~~~~~~~~~~~~~~~~~~~~
Final mood for a journal entry: HealthKit first, the user's own 1-5 tap second.

HealthKit (state_of_mind) is authoritative: exact valence plus labels and
associations. The join is a forward-only window [T, T+WINDOW] from the entry
time, because the entry is created first and the check-in logged afterwards;
the closest record wins, label overlap breaking ties.

Retro entries never join HealthKit: a check-in made now says nothing about how
the covered day felt, so only the user's own tap (if any) is used.
"""

from datetime import datetime, timedelta, timezone
from typing import Optional

WINDOW = timedelta(minutes=45)
_TS_FMT = "%Y-%m-%dT%H:%M:%SZ"


def find_healthkit_mood(conn, ts_utc: datetime, entry_labels=()) -> Optional[dict]:
    rows = conn.execute(
        "SELECT id, start_ts, valence, classification FROM state_of_mind "
        "WHERE start_ts >= ? AND start_ts <= ? ORDER BY start_ts",
        (ts_utc.strftime(_TS_FMT), (ts_utc + WINDOW).strftime(_TS_FMT)),
    ).fetchall()
    if not rows:
        return None

    wanted = {l.lower() for l in entry_labels}

    def labels(som_id):
        return [r[0] for r in conn.execute(
            "SELECT label FROM mood_labels WHERE som_id = ? ORDER BY label", (som_id,))]

    def rank(row):
        ts = datetime.strptime(row[1], _TS_FMT).replace(tzinfo=timezone.utc)
        overlap = len(wanted & {l.lower() for l in labels(row[0])})
        return (abs((ts - ts_utc).total_seconds()), -overlap)

    best = min(rows, key=rank)
    return {
        "som_id": best[0],
        "valence": best[2],
        "classification": best[3],
        "labels": labels(best[0]),
        "associations": [r[0] for r in conn.execute(
            "SELECT association FROM mood_associations WHERE som_id = ? ORDER BY association",
            (best[0],))],
    }


def resolve_mood(conn, row) -> dict:
    """Return {valence, valence_source, valence_class, labels, associations} for an entry row.

    valence_source is 'healthkit', 'self_tap' or None (nothing known).
    """
    import json
    extra = json.loads(row["enrichment_json"] or "{}")
    ts = datetime.strptime(row["ts_utc"], _TS_FMT).replace(tzinfo=timezone.utc)

    hk = None
    if row["provenance"] == "contemporaneous":
        hk = find_healthkit_mood(conn, ts, (extra.get("mood") or {}).get("labels", []))
    if hk:
        return {"valence": hk["valence"], "valence_source": "healthkit",
                "valence_class": hk["classification"],
                "labels": hk["labels"], "associations": hk["associations"]}

    sidecar = extra.get("mood") or {}   # labels carried over from an Apple Journal export
    return {
        "valence": row["valence_self"],
        "valence_source": "self_tap" if row["valence_self"] is not None else None,
        "valence_class": None,
        "labels": sidecar.get("labels", []),
        "associations": sidecar.get("associations", []),
    }
