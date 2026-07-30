"""
scheduled_tasks/daily_summary/pi.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Owns the Pi-infrastructure columns of daily_summary: watchdog heartbeat
aggregates, power consumption, and photo counts.
"""
from config.editable import load_overrides
load_overrides()

from datetime import datetime

from prefect import flow
from prefect.logging import get_run_logger

from notifications import notify_on_completion, log_on_success, record_flow_result
from scheduled_tasks.daily_summary.base import Domain, closed_when_present_else_after


# ---------------------------------------------------------------------------
# Compute function
# ---------------------------------------------------------------------------

def compute_pi_data(conn, ctx: dict) -> dict:
    data = {}
    data.update(_photos(conn, ctx))
    data.update(_watchdog(conn, ctx))
    data.update(_power(conn, ctx))
    return data


def _photos(conn, ctx: dict) -> dict:
    row = conn.execute("""
        SELECT COUNT(*) AS n FROM photo_metadata
        WHERE taken_at >= ? AND taken_at < ?
    """, (ctx["utc_start"], ctx["utc_end"])).fetchone()
    return {"photo_count": row["n"]}


def _watchdog(conn, ctx: dict) -> dict:
    wd = conn.execute("""
        SELECT
            COUNT(*)                     AS n,
            MAX(consecutive_failures)    AS max_fail,
            AVG(CASE WHEN internet_ok = 1 THEN 100.0 ELSE 0.0 END) AS internet_pct,
            AVG(CASE WHEN api_ok      = 1 THEN 100.0 ELSE 0.0 END) AS api_pct,
            AVG(CASE WHEN prefect_ok  = 1 THEN 100.0 ELSE 0.0 END) AS prefect_pct
        FROM watchdog_heartbeat
        WHERE received_at >= ? AND received_at < ?
    """, (ctx["utc_start"], ctx["utc_end"])).fetchone()

    # Longest gap between consecutive heartbeats in the window
    max_gap_mins = None
    if wd["n"] and wd["n"] > 1:
        times = conn.execute("""
            SELECT received_at FROM watchdog_heartbeat
            WHERE received_at >= ? AND received_at < ?
            ORDER BY received_at ASC
        """, (ctx["utc_start"], ctx["utc_end"])).fetchall()
        max_delta = 0.0
        prev = None
        for t in times:
            cur = datetime.fromisoformat(t["received_at"].replace("Z", "+00:00"))
            if prev is not None:
                max_delta = max(max_delta, (cur - prev).total_seconds() / 60)
            prev = cur
        max_gap_mins = int(round(max_delta)) if max_delta else None

    return {
        "watchdog_heartbeats_received":  wd["n"] or 0,
        "watchdog_max_gap_mins":         max_gap_mins,
        "watchdog_max_consecutive_fail": wd["max_fail"],
        "travelnet_internet_ok_pct":     round(wd["internet_pct"], 2) if wd["internet_pct"] is not None else None,
        "travelnet_api_ok_pct":          round(wd["api_pct"], 2)      if wd["api_pct"]      is not None else None,
        "prefect_ok_pct":                round(wd["prefect_pct"], 2)  if wd["prefect_pct"]  is not None else None,
    }


def _power(conn, ctx: dict) -> dict:
    row = conn.execute("""
        SELECT avg_w, total_wh FROM power_daily WHERE date = ?
    """, (ctx["date"],)).fetchone()
    return {
        "avg_w_pi":    row["avg_w"]    if row else None,
        "total_wh_pi": row["total_wh"] if row else None,
    }


# ---------------------------------------------------------------------------
# Domain spec
# ---------------------------------------------------------------------------

# Fallback age (days) after which a date is closed even with no pi data at
# all — covers dates that will genuinely never receive data (Pi offline,
# no backfill possible) so they don't stay pending forever. Chosen larger
# than the old blanket 2-day window since it's now a last resort rather
# than the primary trigger; flagged for review — no strong signal dictated
# this exact value.
_NO_DATA_FALLBACK_DAYS = 14


def _pi_data_present(data: dict) -> bool:
    """True once real watchdog or power data has landed for the date.

    Deliberately OR, not AND — unlike spend_complete (transactions.py),
    which requires BOTH revolut and wise because they're two disjoint
    components of a single total (missing either one understates spend).
    Watchdog and power here are independent, redundant liveness signals
    for the same underlying "is the Pi pipeline alive for this date"
    question, not two halves of one figure — either one landing is
    sufficient evidence the day isn't stuck waiting on ingestion. This
    also avoids one source's legitimate permanent absence (e.g. the power
    plug was unplugged all day) blocking completion until the age fallback.
    """
    return bool(data.get("watchdog_heartbeats_received")) or data.get("avg_w_pi") is not None


PI_DOMAIN = Domain(
    name="pi",
    columns=frozenset({
        "photo_count",
        "watchdog_heartbeats_received", "watchdog_max_gap_mins",
        "watchdog_max_consecutive_fail",
        "travelnet_internet_ok_pct", "travelnet_api_ok_pct",
        "prefect_ok_pct", "avg_w_pi", "total_wh_pi",
    }),
    completeness_flag="pi_complete",
    compute_fn=compute_pi_data,
    # Close as soon as watchdog/power data has actually arrived (previously
    # this closed blindly after 2 days regardless of data presence, which
    # permanently masked gaps and dropped straggler data that arrived late
    # since compute_daily_summary_flow only reprocesses pi_complete = 0
    # dates). Falls back to calendar age only if nothing ever arrives.
    completeness_predicate=closed_when_present_else_after(_NO_DATA_FALLBACK_DAYS, _pi_data_present),
)


@flow(
    name="Compute Daily Summary — Pi",
    on_failure=[notify_on_completion], on_completion=[log_on_success]
)
def compute_pi_flow(local_date: str) -> dict:
    logger = get_run_logger()
    data = PI_DOMAIN.upsert_for_date(local_date)
    logger.info(f"{local_date}: pi domain upserted "
                f"(heartbeats={data.get('watchdog_heartbeats_received')}, "
                f"photos={data.get('photo_count')})")
    result = {"local_date": local_date, **data}
    record_flow_result(result)
    return result