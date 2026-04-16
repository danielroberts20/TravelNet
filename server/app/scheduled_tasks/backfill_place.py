from config.editable import load_overrides
load_overrides()

from datetime import datetime, timezone, timedelta
from notifications import notify_on_completion, record_flow_result
from prefect import task, flow
from prefect.logging import get_run_logger

from database.connection import get_conn, to_iso_str
from notifications import notify_on_completion



_NEAREST_PLACE_SQL = """
    SELECT lu.place_id FROM location_unified lu
    WHERE lu.place_id IS NOT NULL
        AND CAST(strftime('%s', lu.timestamp) AS INTEGER)
            BETWEEN CAST(strftime('%s', :ts) AS INTEGER) - :window_s
                AND CAST(strftime('%s', :ts) AS INTEGER) + :window_s
    ORDER BY
        CASE WHEN lu.timestamp <= :ts THEN 0 ELSE 1 END ASC,
        ABS(strftime('%s', lu.timestamp) - strftime('%s', :ts)) ASC
    LIMIT 1
"""


def _nearest_place_id(conn, ts: str, window_s: int) -> int | None:
    """Return the place_id of the nearest location to ts within window_s seconds.

    Prefers the most recent location before ts; falls back to the earliest after.
    """
    row = conn.execute(_NEAREST_PLACE_SQL, {"ts": ts, "window_s": window_s}).fetchone()
    return row["place_id"] if row else None


@task
def backfill_all_places() -> dict:
    logger = get_run_logger()

    with get_conn() as conn:

        # --- Transactions ---
        transaction_rows = conn.execute("""
            SELECT id, source, currency, timestamp FROM transactions WHERE place_id IS NULL
            """).fetchall()
        logger.info(f"Found {len(transaction_rows)} transactions to backfill place_id for")

        filled_transactions = 0
        for transaction in transaction_rows:
            place_id = _nearest_place_id(conn, transaction["timestamp"], 7200)
            if place_id is not None:
                conn.execute("""
                    UPDATE transactions SET place_id = :place_id
                    WHERE id = :id AND currency = :currency AND source = :source
                    """, {"place_id": place_id,
                        "id": transaction["id"],
                        "currency": transaction["currency"],
                        "source": transaction["source"]})
                filled_transactions += 1
        logger.info(f"Backfilled place_id for {filled_transactions} transactions")

        # --- Health quantity ---
        health_quantity_rows = conn.execute("""
            SELECT id, timestamp FROM health_quantity WHERE place_id IS NULL
            """).fetchall()
        logger.info(f"Found {len(health_quantity_rows)} health quantity entries to backfill place_id for")

        filled_health_quantity = 0
        for health_quantity in health_quantity_rows:
            place_id = _nearest_place_id(conn, health_quantity["timestamp"], 3600)
            if place_id is not None:
                conn.execute("""
                    UPDATE health_quantity SET place_id = :place_id
                    WHERE id = :id
                    """, {"place_id": place_id,
                        "id": health_quantity["id"]})
                filled_health_quantity += 1
        logger.info(f"Backfilled place_id for {filled_health_quantity} health quantity entries")

        # --- Health heart rate ---
        health_heart_rate_rows = conn.execute("""
            SELECT id, timestamp FROM health_heart_rate WHERE place_id IS NULL
            """).fetchall()
        logger.info(f"Found {len(health_heart_rate_rows)} health heart rate entries to backfill place_id for")

        filled_health_heart_rate = 0
        for health_heart_rate in health_heart_rate_rows:
            place_id = _nearest_place_id(conn, health_heart_rate["timestamp"], 3600)
            if place_id is not None:
                conn.execute("""
                    UPDATE health_heart_rate SET place_id = :place_id
                    WHERE id = :id
                    """, {"place_id": place_id,
                        "id": health_heart_rate["id"]})
                filled_health_heart_rate += 1
        logger.info(f"Backfilled place_id for {filled_health_heart_rate} health heart rate entries")

        # --- Health sleep ---
        health_sleep_rows = conn.execute("""
            SELECT id, start_ts, duration_hr FROM health_sleep WHERE place_id IS NULL
            """).fetchall()
        logger.info(f"Found {len(health_sleep_rows)} health sleep entries to backfill place_id for")

        filled_health_sleep = 0
        for health_sleep in health_sleep_rows:
            start = datetime.fromisoformat(health_sleep["start_ts"].replace("Z", "+00:00"))
            target_ts = start + timedelta(hours=health_sleep["duration_hr"] / 2)
            target_ts = to_iso_str(target_ts)
            place_id = _nearest_place_id(conn, target_ts, 1800)
            if place_id is not None:
                conn.execute("""
                    UPDATE health_sleep SET place_id = :place_id
                    WHERE id = :id
                    """, {"place_id": place_id,
                        "id": health_sleep["id"]})
                filled_health_sleep += 1
        logger.info(f"Backfilled place_id for {filled_health_sleep} health sleep entries")

        # --- State of mind ---
        state_of_mind_rows = conn.execute("""
            SELECT id, start_ts FROM state_of_mind WHERE place_id IS NULL
            """).fetchall()
        logger.info(f"Found {len(state_of_mind_rows)} state of mind entries to backfill place_id for")

        filled_state_of_mind = 0
        for state_of_mind in state_of_mind_rows:
            place_id = _nearest_place_id(conn, state_of_mind["start_ts"], 7200)
            if place_id is not None:
                conn.execute("""
                    UPDATE state_of_mind SET place_id = :place_id
                    WHERE id = :id
                    """, {"place_id": place_id,
                        "id": state_of_mind["id"]})
                filled_state_of_mind += 1
        logger.info(f"Backfilled place_id for {filled_state_of_mind} state of mind entries")

        # --- Workouts ---
        workout_rows = conn.execute("""
            SELECT id, start_ts FROM workouts WHERE start_place_id IS NULL
            """).fetchall()
        logger.info(f"Found {len(workout_rows)} workouts to backfill place_id for")

        filled_workouts = 0
        for workout in workout_rows:
            place_id = _nearest_place_id(conn, workout["start_ts"], 900)
            if place_id is not None:
                conn.execute("""
                    UPDATE workouts SET start_place_id = :place_id
                    WHERE id = :id
                    """, {"place_id": place_id,
                        "id": workout["id"]})
                filled_workouts += 1
        logger.info(f"Backfilled place_id for {filled_workouts} workouts")

        # --- Trigger log ---
        trigger_row = conn.execute("""
            SELECT id, fired_at FROM trigger_log WHERE place_id IS NULL
            """).fetchall()
        logger.info(f"Found {len(trigger_row)} trigger log entries to backfill place_id for")

        filled_trigger_log = 0
        for trigger in trigger_row:
            place_id = _nearest_place_id(conn, trigger["fired_at"], 900)
            if place_id is not None:
                conn.execute("""
                    UPDATE trigger_log SET place_id = :place_id
                    WHERE id = :id
                    """, {"place_id": place_id,
                        "id": trigger["id"]})
                filled_trigger_log += 1
        logger.info(f"Backfilled place_id for {filled_trigger_log} trigger log entries")

    return {
        "transactions_found": len(transaction_rows),
        "transactions_backfilled": filled_transactions,
        "health_quantity_found": len(health_quantity_rows),
        "health_quantity_backfilled": filled_health_quantity,
        "health_sleep_found": len(health_sleep_rows),
        "health_sleep_backfilled": filled_health_sleep,
        "health_heart_rate_found": len(health_heart_rate_rows),
        "health_heart_rate_backfilled": filled_health_heart_rate,
        "state_of_mind_found": len(state_of_mind_rows),
        "state_of_mind_backfilled": filled_state_of_mind,
        "workouts_found": len(workout_rows),
        "workouts_backfilled": filled_workouts,
        "trigger_log_found": len(trigger_row),
        "trigger_log_backfilled": filled_trigger_log,
    }


@flow(name="Backfill Place", on_completion=[notify_on_completion], on_failure=[notify_on_completion])
def backfill_place_flow():
    result = backfill_all_places()
    record_flow_result(result)
    return result
