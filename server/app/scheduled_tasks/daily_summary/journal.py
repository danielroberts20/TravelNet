"""
scheduled_tasks/daily_summary/journal.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Owns the journal columns of daily_summary:

    journal_written      1 if any entry covers the day (written at the time or
                         later, i.e. provenance contemporaneous or retro).
                         Unconfirmed `drafted_from_telemetry` entries never count.
    journal_entry_count  how many such entries cover the day
    journal_retro        1 if the day is covered ONLY by retro entries, so
                         analyses can separate real-time journalling from catch-up

An entry covers a day when covers_start <= day <= covers_end.

A day is never "complete" for journal purposes: a retro entry can arrive at any
time. journal_complete is therefore never set, and it is deliberately NOT part
of daily_summary_complete or of the master flow's recompute filter.
"""
from config.editable import load_overrides
load_overrides()

from prefect import flow
from prefect.logging import get_run_logger

from database.connection import get_conn
from notifications import notify_on_completion, log_on_success, record_flow_result
from scheduled_tasks.daily_summary.base import Domain, never_auto_close


def compute_journal_data(conn, ctx: dict) -> dict:
    day = ctx["date"]
    row = conn.execute("""
        SELECT COUNT(*) AS n,
               SUM(CASE WHEN provenance = 'contemporaneous' THEN 1 ELSE 0 END) AS live
        FROM journal_entries
        WHERE provenance IN ('contemporaneous', 'retro')
          AND COALESCE(covers_start, local_date) <= ?
          AND COALESCE(covers_end,   local_date) >= ?
    """, (day, day)).fetchone()
    n = row["n"] or 0
    live = row["live"] or 0
    return {
        "journal_written":     1 if n else 0,
        "journal_entry_count": n,
        "journal_retro":       1 if n and not live else 0,
    }


JOURNAL_DOMAIN = Domain(
    name="journal",
    columns=frozenset({"journal_written", "journal_entry_count", "journal_retro"}),
    completeness_flag="journal_complete",
    compute_fn=compute_journal_data,
    completeness_predicate=never_auto_close,
)


def refresh_journal_days(dates) -> int:
    """Recompute the journal columns for dates that already have a daily_summary row.

    Called when an entry is saved or imported, so a retro entry for an old day is
    reflected immediately instead of waiting for the (windowed) nightly flow.
    Dates without a row are skipped: the master flow creates those rows itself.
    """
    n = 0
    for d in sorted(set(dates)):
        with get_conn(read_only=True) as conn:
            exists = conn.execute("SELECT 1 FROM daily_summary WHERE date = ?", (d,)).fetchone()
        if exists:
            JOURNAL_DOMAIN.upsert_for_date(d)
            n += 1
    return n


@flow(
    name="Compute Daily Summary — Journal",
    on_failure=[notify_on_completion], on_completion=[log_on_success],
)
def compute_journal_flow(local_date: str) -> dict:
    logger = get_run_logger()
    data = JOURNAL_DOMAIN.upsert_for_date(local_date)
    logger.info(f"{local_date}: journal domain upserted "
                f"(written={data.get('journal_written')}, entries={data.get('journal_entry_count')})")
    result = {"local_date": local_date, **data}
    record_flow_result(result)
    return result
