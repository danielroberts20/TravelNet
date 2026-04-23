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

from notifications import notify_on_completion, record_flow_result
from scheduled_tasks.daily_summary.base import Domain, closed_after


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
            AVG(CASE WHEN api_ok      = 1 THEN 100.0 ELSE 0.0 END) AS api_pct
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

PI_DOMAIN = Domain(
    name="pi",
    columns=frozenset({
        "photo_count",
        "watchdog_heartbeats_received", "watchdog_max_gap_mins",
        "watchdog_max_consecutive_fail",
        "travelnet_internet_ok_pct", "travelnet_api_ok_pct",
        "avg_w_pi", "total_wh_pi",
    }),
    completeness_flag="pi_complete",
    compute_fn=compute_pi_data,
    # Watchdog and power data are realtime, photos are daily — 2 days is plenty.
    completeness_predicate=closed_after(2),
)


@flow(
    name="Compute Daily Summary — Pi"
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