"""
Nightly reconcile of journal_entries into Trevor's vector store.

Entries are pushed to Trevor as they are saved; this flow is the safety net. It
sends the complete set so Trevor can (re)index anything a failed push missed and
drop documents for rows that were deleted. Idempotent.
"""
from config.editable import load_overrides
load_overrides()

from prefect import flow
from prefect.logging import get_run_logger

from database.journal.sync import reconcile_all
from notifications import notify_on_completion, log_on_success, record_flow_result


@flow(name="Sync Journal To Trevor", on_failure=[notify_on_completion], on_completion=[log_on_success])
def sync_journal_to_trevor_flow() -> dict:
    logger = get_run_logger()
    sent = reconcile_all()
    if sent is None:
        # Empty table is also None; distinguish so a quiet day does not page.
        from database.connection import get_conn
        with get_conn(read_only=True) as conn:
            total = conn.execute("SELECT COUNT(*) FROM journal_entries").fetchone()[0]
        if total == 0:
            logger.info("No journal entries to reconcile")
            result = {"entries": 0, "reconciled": False}
            record_flow_result(result)
            return result
        raise RuntimeError("Journal reconcile to Trevor failed (see logs)")
    logger.info("Reconciled %d journal entries with Trevor", sent)
    result = {"entries": sent, "reconciled": True}
    record_flow_result(result)
    return result
