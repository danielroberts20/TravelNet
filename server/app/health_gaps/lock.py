"""
health_gaps/lock.py
~~~~~~~~~~~~~~~~~~~
A non-blocking single-run lock for the recovery flow.

Two recovery runs at once both pull the same gaps from the phone (HAE serves requests one at a
time, so each slows the other), double the requests, inflate pull_attempts, and each sends its own
"done" notification. The inserts are idempotent so no data is harmed, but it is wasteful and
confusing. ``flock`` is released by the kernel if the process dies, so a crashed run never leaves
the lock stuck.
"""

from __future__ import annotations

import fcntl
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


@contextmanager
def single_run(path: Path) -> Iterator[bool]:
    """Yield True if this caller now holds the lock, False if another run holds it."""
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = open(path, "a+")
    acquired = False
    try:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            acquired = True
        except BlockingIOError:
            acquired = False
        yield acquired
    finally:
        if acquired:
            fcntl.flock(handle, fcntl.LOCK_UN)
        handle.close()
