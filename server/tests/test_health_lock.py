"""Tests for health_gaps/lock.py: the single-run lock for the recovery flow."""

import multiprocessing
import time

from health_gaps.lock import single_run


def test_first_caller_acquires(tmp_path):
    with single_run(tmp_path / "r.lock") as acquired:
        assert acquired is True


def test_second_caller_is_refused_while_the_first_holds_it(tmp_path):
    path = tmp_path / "r.lock"
    with single_run(path) as first:
        assert first is True
        with single_run(path) as second:
            assert second is False
        with single_run(path) as third:             # still refused; a refused caller must not free it
            assert third is False


def test_lock_is_released_on_exit_even_after_an_error(tmp_path):
    path = tmp_path / "r.lock"
    try:
        with single_run(path) as acquired:
            assert acquired
            raise RuntimeError("run failed")
    except RuntimeError:
        pass
    with single_run(path) as again:
        assert again is True


def test_creates_the_parent_directory(tmp_path):
    with single_run(tmp_path / "a" / "b" / "r.lock") as acquired:
        assert acquired is True


def _hold(path, ready, release):
    with single_run(path) as acquired:
        ready.put(acquired)
        release.get(timeout=10)


def test_lock_excludes_another_process_and_frees_when_it_dies(tmp_path):
    path = tmp_path / "r.lock"
    ctx = multiprocessing.get_context("fork")
    ready, release = ctx.Queue(), ctx.Queue()
    p = ctx.Process(target=_hold, args=(path, ready, release))
    p.start()
    assert ready.get(timeout=10) is True
    with single_run(path) as acquired:
        assert acquired is False                      # held by the other process
    p.kill()                                           # simulate a crashed run: no cleanup runs
    p.join(5)
    deadline = time.time() + 5
    while time.time() < deadline:
        with single_run(path) as acquired:
            if acquired:
                break
        time.sleep(0.1)
    assert acquired is True                            # the kernel released it; never stuck
