"""Tests for health_gaps/recovery.py (notify, then poll for HAE's MCP server)."""

import socket
import threading

import pytest

from health_gaps.recovery import (
    FAST_INTERVAL_S, FAST_WINDOW_S, SLOW_INTERVAL_S,
    interval_for, notify_and_wait, server_is_up,
)


class FakeTime:
    def __init__(self):
        self.t = 0.0
        self.sleeps: list[float] = []

    def clock(self):
        return self.t

    def sleep(self, s):
        self.sleeps.append(s)
        self.t += s


def run(probe, *, notify=lambda: None, deadline_s=60.0):
    ft = FakeTime()
    res = notify_and_wait(notify, "h", 1, deadline_s=deadline_s, probe=probe,
                          sleep=ft.sleep, clock=ft.clock)
    return res, ft


def test_interval_speeds():
    assert interval_for(0) == FAST_INTERVAL_S
    assert interval_for(FAST_WINDOW_S - 1) == FAST_INTERVAL_S
    assert interval_for(FAST_WINDOW_S) == SLOW_INTERVAL_S


def test_notifies_before_first_probe():
    order = []
    run(lambda h, p: order.append("probe") or True, notify=lambda: order.append("notify"))
    assert order == ["notify", "probe"]


def test_up_on_first_probe():
    res, ft = run(lambda h, p: True)
    assert res.up and res.probes == 1 and res.elapsed_s == 0 and ft.sleeps == []


def test_up_after_a_few_probes_polls_at_fast_interval():
    answers = iter([False, False, False, True])
    res, ft = run(lambda h, p: next(answers))
    assert res.up and res.probes == 4
    assert ft.sleeps == [FAST_INTERVAL_S] * 3
    assert res.elapsed_s == pytest.approx(3 * FAST_INTERVAL_S)


def test_gives_up_at_deadline():
    res, ft = run(lambda h, p: False, deadline_s=10)
    assert not res.up and res.elapsed_s >= 10
    assert res.probes == 10          # 1s cadence, no probe after the deadline


def test_slows_down_after_fast_window():
    res, ft = run(lambda h, p: False, deadline_s=FAST_WINDOW_S + 20)
    assert not res.up
    assert ft.sleeps[0] == FAST_INTERVAL_S and ft.sleeps[-1] == SLOW_INTERVAL_S
    assert all(s in (FAST_INTERVAL_S, SLOW_INTERVAL_S) for s in ft.sleeps)


def test_sleep_never_overshoots_deadline():
    res, ft = run(lambda h, p: False, deadline_s=2.5)
    assert ft.t == pytest.approx(2.5) and ft.sleeps == [1.0, 1.0, 0.5]


def test_notify_failure_still_polls():
    def boom():
        raise RuntimeError("pushcut down")
    res, _ = run(lambda h, p: True, notify=boom)
    assert res.up and res.notified is False


def test_server_is_up_real_socket():
    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)
    port = srv.getsockname()[1]
    t = threading.Thread(target=lambda: srv.accept()[0].close(), daemon=True)
    t.start()
    assert server_is_up("127.0.0.1", port, timeout=1)
    t.join(2)
    srv.close()
    assert not server_is_up("127.0.0.1", port, timeout=0.5)
