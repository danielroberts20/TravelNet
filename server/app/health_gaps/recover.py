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
from typing import Callable, ContextManager, Optional

from health_gaps import events as ev_mod
from health_gaps.config import config_for
from health_gaps.events import Event
from health_gaps.hae_client import HAEAuthError, HAEClient, HAEError, HAEProtocolError, HAEUnreachable
from health_gaps.pull import EventPull, estimate_requests, pull_event
from health_gaps.recovery import WaitResult

logger = logging.getLogger(__name__)

NOTHING_TO_DO, COOLDOWN, DIGEST_ONLY, TIMEOUT, DONE, INTERRUPTED, AUTH_ERROR, ERROR = (
    "nothing_to_do", "cooldown", "digest_only", "timeout", "done", "interrupted", "auth_error", "error",
)


@dataclass
class RecoveryParams:
    pad_h: float = 12.0
    cooldown_h: float = 20.0         # below the daily schedule period, so schedule jitter never skips a day
    digest_weekday: Optional[int] = None   # 0=Mon; on this day digest-only gaps (except sparse ones) may prompt
    force: bool = False              # manual run: ignore the cooldown and prompt for any gap
    max_requests: Optional[int] = None     # session budget; whole events only (None/0 = unlimited)
    max_tier: int = 3                # skip gaps in tiers above this (1 loud, 2 walking, 3 sparse-only)
    dry_run: bool = False            # pull and count, but ingest and record nothing


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
    waited_s: float = 0.0                                   # how long we waited for the server (TIMEOUT)
    remaining: list[int] = field(default_factory=list)     # not attempted this session (budget / interruption)
    dry_run: bool = False


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
    if o.status == TIMEOUT:
        minutes = max(1, round(o.waited_s / 60))
        return ("🩺 Health Recovery Timed Out",
                f"⏱️ The HAE server didn't start within {minutes} min, so nothing was recovered. "
                f"I'll try again tomorrow.")
    if o.dry_run:
        more = f" ({len(o.remaining)} more gaps not pulled this session)" if o.remaining else ""
        return ("🩺 Health Recovery Dry Run",
                f"✅ Would add up to {o.points} points across {len(o.pulled)} gaps{more}. "
                f"Nothing was saved. You can safely stop the HAE server now.")
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
    if o.remaining:
        n = len(o.remaining)
        return ("🩺 Health Recovery Session Done",
                f"✅ Session done: {summary}. {n} more gap{'s' if n != 1 else ''} remain for the next session. "
                f"You can safely stop the HAE server now.")
    return ("🩺 Health Recovery Finished", f"✅ All done: {summary}. You can safely stop the HAE server now.")


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------

def worth_prompting(event: Event, is_digest_day: bool) -> bool:
    """Is this gap worth interrupting the user for?

    Gaps with a loud (immediate) metric always are. Digest-only gaps, e.g. the walking
    metrics, are only on the weekly digest day, so they are still recovered without a
    prompt every day. A gap made up solely of sparse metrics (VO2 Max, ...) never is: the
    phone rarely has more than the DB, so it only rides along when a prompt happens anyway.
    """
    if event.immediate:
        return True
    cfgs = [config_for(m) for m in event.metrics]
    return is_digest_day and any(c is not None and not c.sparse for c in cfgs)


def recovery_tier(event: Event) -> int:
    """1 = a loud metric is involved, 2 = digest-only (walking...), 3 = sparse metrics only."""
    if event.immediate:
        return 1
    cfgs = [config_for(m) for m in event.metrics]
    if cfgs and all(c is not None and c.sparse for c in cfgs):
        return 3
    return 2


def run_recovery(state_conn: sqlite3.Connection, deps: RecoveryDeps, params: RecoveryParams = RecoveryParams()) -> RecoveryOutcome:
    now = deps.now()
    pending = [e for e in ev_mod.needing_recovery(state_conn) if recovery_tier(e) <= params.max_tier]
    if not pending:
        return RecoveryOutcome(NOTHING_TO_DO)

    # Only gaps worth interrupting the user for may start a prompt; every other pending gap
    # rides along whenever the server is up anyway.
    is_digest_day = params.digest_weekday is not None and now.weekday() == params.digest_weekday
    promptable = [e for e in pending if worth_prompting(e, is_digest_day)]
    due = ev_mod.due_for_prompt(promptable, now, params.cooldown_h)
    if not due and not params.force:
        if not promptable:
            return RecoveryOutcome(DIGEST_ONLY, detail=f"{len(pending)} gap(s) pending, none worth a prompt today")
        return RecoveryOutcome(COOLDOWN, detail=f"{len(pending)} gap(s) pending, prompted recently")

    if not deps.server_up():
        if not params.dry_run:
            ev_mod.mark_prompted(state_conn, [e.id for e in pending], now)
        wait = deps.wait_for_server(lambda: deps.prompt(pending))
        logger.info("Recovery: server %s after %.0fs (%d probes)",
                    "up" if wait.up else "NOT up", wait.elapsed_s, wait.probes)
        if not wait.up:
            out = RecoveryOutcome(TIMEOUT, detail=f"no answer after {wait.elapsed_s:.0f}s", waited_s=wait.elapsed_s)
            deps.finish(*finish_text(out))
            return out
    else:
        logger.info("Recovery: HAE server already up; skipping the notification")

    outcome = _pull_all(state_conn, deps, params, pending)
    if outcome.pulled and not params.dry_run:
        deps.rescan(deps.now())
        _classify(state_conn, outcome)
    title, body = finish_text(outcome)
    deps.finish(title, body)
    return outcome


def _pull_all(state_conn, deps: RecoveryDeps, params: RecoveryParams, pending: list[Event]) -> RecoveryOutcome:
    out = RecoveryOutcome(DONE, dry_run=params.dry_run)
    # Loud gaps first, then walking, then sparse; newest first within a tier.
    ordered = sorted(pending, key=lambda e: (recovery_tier(e), -e.window_start.timestamp()))
    ingest = (lambda data: None) if params.dry_run else deps.ingest
    used = 0
    try:
        with deps.make_client() as client:
            for i, event in enumerate(ordered):
                cost = estimate_requests(event, deps.now(), params.pad_h)
                if params.max_requests and out.pulled and used + cost > params.max_requests:
                    out.remaining = [e.id for e in ordered[i:]]        # leave whole events for the next session
                    break
                result: EventPull = pull_event(client, event, deps.now(), ingest, pad_h=params.pad_h)
                if not params.dry_run:
                    ev_mod.record_pull(state_conn, event.id, deps.now(), complete=result.complete)
                used += cost
                out.pulled.append(event.id)
                out.points += result.points
                if result.interrupted:
                    out.status = INTERRUPTED
                    out.remaining = [e.id for e in ordered[i + 1:]]
                    break
    except HAEAuthError:
        out.status = AUTH_ERROR
    except HAEUnreachable as e:                 # lost it at the handshake or between events
        logger.warning("Recovery: phone unreachable: %s", e)
        out.status = INTERRUPTED
        out.remaining = [e.id for e in ordered if e.id not in out.pulled]
    except HAEProtocolError as e:
        logger.error("Recovery: unusable response from HAE: %s", e)
        out.status, out.detail = ERROR, str(e)[:120]
    except HAEError as e:                       # defensive: any other client error
        out.status, out.detail = ERROR, str(e)[:120]
    logger.info("Recovery: pulled %d event(s), ~%d request(s), %d remaining", len(out.pulled), used, len(out.remaining))
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
