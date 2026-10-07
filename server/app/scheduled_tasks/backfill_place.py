from config.editable import load_overrides
load_overrides()

from datetime import datetime, timedelta
from typing import Callable, NamedTuple

from notifications import notify_on_completion, log_on_success, record_flow_result
from prefect import task, flow
from prefect.logging import get_run_logger

from database.connection import get_conn, to_iso_str
from database.location.nearest_place import nearest_place_id

# Rows written per transaction. The write lock is only ever held for one chunk.
_WRITE_CHUNK = 500


class _Target(NamedTuple):
    """One table whose rows are matched to the nearest location fix."""
    key: str                                  # result-dict prefix and log label
    description: str
    select_sql: str                           # rows still missing a place
    window_s: int                             # max distance (seconds) to a location fix
    update_sql: str                           # named params, see params_of
    ts_of: Callable[[object], str]            # row -> timestamp to look up
    params_of: Callable[[object, int], dict]  # (row, place_id) -> update params


def _sleep_midpoint(row) -> str:
    start = datetime.fromisoformat(row["start_ts"].replace("Z", "+00:00"))
    return to_iso_str(start + timedelta(hours=row["duration_hr"] / 2))


def _by_id(row, place_id: int) -> dict:
    return {"place_id": place_id, "id": row["id"]}


# Order matches the original flow. Each UPDATE also requires the column to
# still be NULL so a row filled by someone else between our read and write is
# never overwritten.
_TARGETS: list[_Target] = [
    _Target(
        "transactions", "transactions",
        "SELECT id, source, currency, timestamp FROM transactions WHERE place_id IS NULL",
        7200,
        """UPDATE transactions SET place_id = :place_id
           WHERE id = :id AND currency = :currency AND source = :source AND place_id IS NULL""",
        lambda r: r["timestamp"],
        lambda r, p: {"place_id": p, "id": r["id"], "currency": r["currency"], "source": r["source"]},
    ),
    _Target(
        "health_quantity", "health quantity entries",
        "SELECT id, timestamp FROM health_quantity WHERE place_id IS NULL",
        3600,
        "UPDATE health_quantity SET place_id = :place_id WHERE id = :id AND place_id IS NULL",
        lambda r: r["timestamp"], _by_id,
    ),
    _Target(
        "health_heart_rate", "health heart rate entries",
        "SELECT id, timestamp FROM health_heart_rate WHERE place_id IS NULL",
        3600,
        "UPDATE health_heart_rate SET place_id = :place_id WHERE id = :id AND place_id IS NULL",
        lambda r: r["timestamp"], _by_id,
    ),
    _Target(
        "health_sleep", "health sleep entries",
        "SELECT id, start_ts, duration_hr FROM health_sleep WHERE place_id IS NULL",
        1800,
        "UPDATE health_sleep SET place_id = :place_id WHERE id = :id AND place_id IS NULL",
        _sleep_midpoint, _by_id,
    ),
    _Target(
        "state_of_mind", "state of mind entries",
        "SELECT id, start_ts FROM state_of_mind WHERE place_id IS NULL",
        7200,
        "UPDATE state_of_mind SET place_id = :place_id WHERE id = :id AND place_id IS NULL",
        lambda r: r["start_ts"], _by_id,
    ),
    _Target(
        "workouts", "workouts",
        "SELECT id, start_ts FROM workouts WHERE start_place_id IS NULL",
        900,
        "UPDATE workouts SET start_place_id = :place_id WHERE id = :id AND start_place_id IS NULL",
        lambda r: r["start_ts"], _by_id,
    ),
    _Target(
        "trigger_log", "trigger log entries",
        "SELECT id, fired_at FROM trigger_log WHERE place_id IS NULL",
        900,
        "UPDATE trigger_log SET place_id = :place_id WHERE id = :id AND place_id IS NULL",
        lambda r: r["fired_at"], _by_id,
    ),
    _Target(
        "photo_metadata", "photo metadata entries",
        "SELECT id, taken_at FROM photo_metadata WHERE place_id IS NULL",
        900,
        "UPDATE photo_metadata SET place_id = :place_id WHERE id = :id AND place_id IS NULL",
        lambda r: r["taken_at"], _by_id,
    ),
]


def resolve_places(conn, targets: list[_Target] = _TARGETS, logger=None) -> dict[str, tuple[int, list[dict]]]:
    """Read phase: find a place for every unmatched row. Takes no write lock.

    Returns ``{key: (rows_found, [update_params, ...])}``. Pass a read-only
    connection (row_factory = sqlite3.Row).
    """
    resolved = {}
    for t in targets:
        rows = conn.execute(t.select_sql).fetchall()
        if logger:
            logger.info(f"Found {len(rows)} {t.description} to backfill place_id for")
        updates = []
        for row in rows:
            place_id = nearest_place_id(conn, t.ts_of(row), t.window_s)
            if place_id is not None:
                updates.append(t.params_of(row, place_id))
        resolved[t.key] = (len(rows), updates)
    return resolved


def apply_updates(resolved: dict[str, tuple[int, list[dict]]], targets: list[_Target] = _TARGETS,
                  logger=None) -> None:
    """Write phase: short transactions (one chunk each), so the write lock is
    held for milliseconds and never across slow reads."""
    for t in targets:
        _, updates = resolved[t.key]
        for i in range(0, len(updates), _WRITE_CHUNK):
            conn = get_conn()
            try:
                with conn:  # one transaction per chunk: commit on success, rollback on error
                    conn.executemany(t.update_sql, updates[i:i + _WRITE_CHUNK])
            finally:
                conn.close()
        if logger:
            logger.info(f"Backfilled place_id for {len(updates)} {t.description}")


@task
def backfill_all_places() -> dict:
    logger = get_run_logger()

    read_conn = get_conn(read_only=True)
    try:
        resolved = resolve_places(read_conn, logger=logger)
    finally:
        read_conn.close()

    apply_updates(resolved, logger=logger)

    result = {}
    for t in _TARGETS:
        found, updates = resolved[t.key]
        result[f"{t.key}_found"] = found
        result[f"{t.key}_backfilled"] = len(updates)
    return result


@flow(name="Backfill Place", on_failure=[notify_on_completion], on_completion=[log_on_success])
def backfill_place_flow():
    result = backfill_all_places()
    record_flow_result(result)
    return result
