"""
health_gaps/recover.py
~~~~~~~~~~~~~~~~~~~~~~
Orchestrates one recovery run: notify -> wait for HAE's server -> pull -> rescan -> report.

    needing_recovery() ──> (cooldown) ──> notification ──> poll for the phone's server
        ──> pull each gap newest-first, record_pull() ──> rescan ──> "all done" message

Every external effect (probe, notification, client, ingest, rescan, final message) is
injected through ``RecoveryDeps`` so each safety rule can be tested without a phone:

  * a dropped connection never marks anything missing (record_pull(complete=False));
  * a bad token aborts with a message, it does not retry;
  * the user is always told the outcome, and told when it is safe to stop the HAE server
    (HAE's server cannot be stopped remotely: there is no stop tool and a session DELETE is 405).

Free of Prefect and of the settings object.
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, ContextManager

from health_gaps import events as ev_mod
from health_gaps.events import Event
from health_gaps.hae_client import HAEAuthError, HAEClient, HAEError, HAEProtocolError, HAEUnreachable
from health_gaps.pull import EventPull, pull_event
from health_gaps.recovery import WaitResult

logger = logging.getLogger(__name__)

NOTHING_TO_DO, COOLDOWN, TIMEOUT, DONE, INTERRUPTED, AUTH_ERROR, ERROR = (
    "nothing_to_do", "cooldown", "timeout", "done", "interrupted", "auth_error", "error",
)


@dataclass
class RecoveryParams:
    pad_h: float = 12.0
    cooldown_h: float = 24.0
    force: bool = False              # manual run: ignore the notification cooldown


@dataclass
class RecoveryDeps:
    now: Callable[[], datetime]
    server_up: Callable[[], bool]                                   # one cheap probe
    wait_for_server: Callable[[Callable[[], object]], WaitResult]   # fires the callable, then polls
    prompt: Callable[[list[Event]], object]                         # the "start the HAE server" notification
    make_client: Callable[[], ContextManager[HAEClient]]
    ingest: Callable[[dict], None]
    rescan: Callable[[datetime], None]                              # detect + reconcile with fresh data
    finish: Callable[[str, str], None]                              # (title, body) to the user


@dataclass
class RecoveryOutcome:
    status: str
    pulled: list[int] = field(default_factory=list)        # events with a pull attempt this run
    recovered: list[int] = field(default_factory=list)     # now resolved
    missing: list[int] = field(default_factory=list)       # now unrecoverable (phone had nothing)
    pending: list[int] = field(default_factory=list)       # still confirmed
    points: int = 0
    detail: str = ""


# ---------------------------------------------------------------------------
# message text
# ---------------------------------------------------------------------------

def prompt_text(events: list[Event]) -> tuple[str, str]:
    n = len(events)
    return (
        "🩺 Health Data Gap",
        f"📲 {n} health gap{'s' if n != 1 else ''} can be recovered. Tap to start the HAE server, "
        f"then leave the phone unlocked until you're told it's done.",
    )


def finish_text(o: RecoveryOutcome) -> tuple[str, str]:
    if o.status == AUTH_ERROR:
        return ("🩺 Health Recovery Failed",
                "❌ The HAE server rejected the token. Update HAE_TOKEN, then stop the HAE server.")
    if o.status == ERROR:
        return ("🩺 Health Recovery Failed",
                f"❌ {o.detail or 'Unexpected response from the HAE server.'} You can stop the HAE server.")
    parts = []
    if o.recovered:
        parts.append(f"{len(o.recovered)} recovered")
    if o.missing:
        parts.append(f"{len(o.missing)} genuinely missing")
    if o.pending:
        parts.append(f"{len(o.pending)} still pending")
    summary = ", ".join(parts) or "nothing to change"
    if o.status == INTERRUPTED:
        return ("🩺 Health Recovery Interrupted",
                f"⚠️ The phone stopped answering part-way ({summary}). Unfinished gaps will be retried. "
                f"Check that the HAE server is stopped.")
    return ("🩺 Health Recovery Finished", f"✅ All done: {summary}. You can safely stop the HAE server now.")


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------

def run_recovery(state_conn: sqlite3.Connection, deps: RecoveryDeps, params: RecoveryParams = RecoveryParams()) -> RecoveryOutcome:
    now = deps.now()
    pending = ev_mod.needing_recovery(state_conn)
    if not pending:
        return RecoveryOutcome(NOTHING_TO_DO)

    due = ev_mod.due_for_prompt(pending, now, params.cooldown_h)
    if not due and not params.force:
        return RecoveryOutcome(COOLDOWN, detail=f"{len(pending)} gap(s) pending, prompted recently")

    if not deps.server_up():
        ev_mod.mark_prompted(state_conn, [e.id for e in pending], now)
        wait = deps.wait_for_server(lambda: deps.prompt(pending))
        logger.info("Recovery: server %s after %.0fs (%d probes)",
                    "up" if wait.up else "NOT up", wait.elapsed_s, wait.probes)
        if not wait.up:
            return RecoveryOutcome(TIMEOUT, detail=f"no answer after {wait.elapsed_s:.0f}s")
    else:
        logger.info("Recovery: HAE server already up; skipping the notification")

    outcome = _pull_all(state_conn, deps, params, pending)
    if outcome.pulled:
        deps.rescan(deps.now())
        _classify(state_conn, outcome)
    title, body = finish_text(outcome)
    deps.finish(title, body)
    return outcome


def _pull_all(state_conn, deps: RecoveryDeps, params: RecoveryParams, pending: list[Event]) -> RecoveryOutcome:
    out = RecoveryOutcome(DONE)
    try:
        with deps.make_client() as client:
            for event in sorted(pending, key=lambda e: e.window_start, reverse=True):   # newest first
                result: EventPull = pull_event(client, event, deps.now(), deps.ingest, pad_h=params.pad_h)
                ev_mod.record_pull(state_conn, event.id, deps.now(), complete=result.complete)
                out.pulled.append(event.id)
                out.points += result.points
                if result.interrupted:
                    out.status = INTERRUPTED
                    break
    except HAEAuthError:
        out.status = AUTH_ERROR
    except HAEUnreachable as e:                 # lost it at the handshake or between events
        logger.warning("Recovery: phone unreachable: %s", e)
        out.status = INTERRUPTED
    except HAEProtocolError as e:
        logger.error("Recovery: unusable response from HAE: %s", e)
        out.status, out.detail = ERROR, str(e)[:120]
    except HAEError as e:                       # defensive: any other client error
        out.status, out.detail = ERROR, str(e)[:120]
    return out


def _classify(state_conn, out: RecoveryOutcome) -> None:
    for event_id in out.pulled:
        event = ev_mod.get_event(state_conn, event_id)
        if event is None:
            continue
        if event.status == ev_mod.RESOLVED:
            out.recovered.append(event_id)
        elif event.status == ev_mod.UNRECOVERABLE:
            out.missing.append(event_id)
        else:
            out.pending.append(event_id)
