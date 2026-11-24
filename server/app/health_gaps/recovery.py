"""
health_gaps/recovery.py
~~~~~~~~~~~~~~~~~~~~~~~
Detecting when Health Auto Export's MCP server is up on the phone.

The server only runs while HAE is foregrounded, so recovery is driven by a
Pushcut notification: the user taps it, a Shortcut starts the server, and we
notice by probing the TCP port. The probe is a bare connect (no HTTP, no
token), so it costs the Pi next to nothing.

Kept free of Prefect and of the settings object so it can be tested directly;
callers pass the host/port and the notify callable.
"""

from __future__ import annotations

import socket
import time
from dataclasses import dataclass
from typing import Callable, Optional

FAST_INTERVAL_S = 1.0     # right after the notification: the user is likely to react quickly
SLOW_INTERVAL_S = 5.0     # after FAST_WINDOW_S: still waiting, no need to hammer
FAST_WINDOW_S = 300.0
CONNECT_TIMEOUT_S = 1.5   # a phone off the tailnet black-holes SYNs; cap each probe
DEFAULT_DEADLINE_S = 1800.0


def server_is_up(host: str, port: int, timeout: float = CONNECT_TIMEOUT_S) -> bool:
    """True if something accepts a TCP connection on host:port."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def interval_for(elapsed_s: float) -> float:
    return FAST_INTERVAL_S if elapsed_s < FAST_WINDOW_S else SLOW_INTERVAL_S


@dataclass(frozen=True)
class WaitResult:
    up: bool
    notified: bool           # the notify callable ran without raising
    elapsed_s: float         # from just before the notification fired
    probes: int


def notify_and_wait(
    notify: Callable[[], object],
    host: str,
    port: int,
    *,
    deadline_s: float = DEFAULT_DEADLINE_S,
    probe: Callable[[str, int], bool] = server_is_up,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
    on_probe: Optional[Callable[[int, float, bool], None]] = None,
) -> WaitResult:
    """Fire the notification, then poll until the server answers or the deadline passes.

    The clock starts immediately before ``notify`` so the deadline and elapsed time are
    measured from the Pi's point of view of when the user was asked.
    """
    t0 = clock()
    notified = True
    try:
        notify()
    except Exception:
        notified = False   # still poll: the user may act on an earlier notification

    probes = 0
    while True:
        elapsed = clock() - t0
        if elapsed >= deadline_s:
            return WaitResult(False, notified, elapsed, probes)
        up = probe(host, port)
        probes += 1
        elapsed = clock() - t0
        if on_probe:
            on_probe(probes, elapsed, up)
        if up:
            return WaitResult(True, notified, elapsed, probes)
        sleep(min(interval_for(elapsed), max(deadline_s - elapsed, 0)))
