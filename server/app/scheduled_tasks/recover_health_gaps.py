"""
Recover confirmed health data gaps by pulling them from the phone's HAE MCP server.

Called as a subflow by the daily check (scheduled_tasks/check_health_gaps.py) and runnable
on its own (Prefect deployment 'recover-health-gaps', or ./runcron.sh):

  1. Select confirmed gaps a pull could still help (events.needing_recovery). Gaps with a
     loud metric start a notification (subject to a cooldown); digest-only gaps such as the
     walking metrics do so only on the weekly digest day; gaps of sparse metrics only ride
     along when the server is up. force=True ignores these rules.
  2. Send the Pushcut notification whose Shortcut starts HAE's MCP server on the phone, and
     poll the phone's port from that moment until it answers (health_gaps/recovery.py).
  3. Pull each gap newest-first, ingest through the normal health upload path (with a JSONL
     backup), rescan, and mark each gap resolved / genuinely missing / still pending.
  4. Tell the user the outcome: done (safe to stop the HAE server by hand, since it cannot be
     stopped remotely), interrupted, failed, or timed out ("I'll try again tomorrow").

All of the logic lives in health_gaps/recover.py; this module only wires real dependencies.
"""

from __future__ import annotations

from datetime import datetime, timezone

from prefect import flow, get_run_logger
from config.editable import load_overrides

load_overrides()

from config.general import (
    HEALTH_BACKUP_DIR,
    HEALTH_GAP_DIGEST_WEEKDAY,
    HEALTH_GAP_PULL_PAD_HOURS,
    HEALTH_GAP_RECOVERY_COOLDOWN_HOURS,
    HEALTH_GAP_RECOVERY_WAIT_MINUTES,
)
from config.settings import settings
from database.connection import get_conn
from database.health.gap_events.table import table as gap_events_table
from health_gaps.hae_client import HAEClient
from health_gaps.ingest import make_ingest
from health_gaps.recover import RecoveryDeps, RecoveryParams, prompt_text, run_recovery
from health_gaps.recovery import notify_and_wait, server_is_up
from health_gaps.scan import recovery_configured, scan_and_notify
from notifications import (
    health_gap_recovery_notification, log_on_success, notify_on_completion,
    record_flow_result, send_notification,
)


@flow(name="Recover Health Gaps", on_failure=[notify_on_completion], on_completion=[log_on_success])
def recover_health_gaps_flow(force: bool = False) -> dict:
    """Pull confirmed health gaps from the phone. ``force`` ignores the notification cooldown."""
    log = get_run_logger()
    if not recovery_configured():
        log.warning("Recovery is not configured (HAE_HOST / HAE_TOKEN / HEALTH_GAP_NOTIFICATION); skipping.")
        return {"status": "disabled"}

    host, port, token = settings.hae_host, settings.hae_port, settings.hae_token

    def wait_for_server(notify):
        def on_probe(n, elapsed, up):
            if n == 1 or up or n % 30 == 0:
                log.info("Probe %d at %.0fs: %s", n, elapsed, "UP" if up else "down")
        return notify_and_wait(notify, host, port,
                               deadline_s=HEALTH_GAP_RECOVERY_WAIT_MINUTES * 60, on_probe=on_probe)

    deps = RecoveryDeps(
        now=lambda: datetime.now(timezone.utc),
        server_up=lambda: server_is_up(host, port),
        wait_for_server=wait_for_server,
        prompt=lambda events: health_gap_recovery_notification(*prompt_text(events)),
        make_client=lambda: HAEClient(host, port, token),
        ingest=make_ingest(HEALTH_BACKUP_DIR),
        rescan=lambda now: scan_and_notify(log, now),
        finish=lambda title, body: send_notification(title=title, body=body, time_sensitive=False),
    )
    params = RecoveryParams(
        pad_h=HEALTH_GAP_PULL_PAD_HOURS,
        cooldown_h=HEALTH_GAP_RECOVERY_COOLDOWN_HOURS,
        digest_weekday=HEALTH_GAP_DIGEST_WEEKDAY,
        force=force,
    )

    gap_events_table.init()   # idempotent; makes sure the pull columns exist
    state_conn = get_conn()
    try:
        outcome = run_recovery(state_conn, deps, params)
    finally:
        state_conn.close()

    result = {
        "status": outcome.status,
        "pulled": len(outcome.pulled),
        "recovered": len(outcome.recovered),
        "missing": len(outcome.missing),
        "pending": len(outcome.pending),
        "points": outcome.points,
    }
    log.info("Health gap recovery: %s", result)
    record_flow_result(result)
    return result
