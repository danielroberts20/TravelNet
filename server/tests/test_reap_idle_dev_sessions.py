# tests/test_reap_idle_dev_sessions.py
#
# The decision logic runs against a fake /proc tree (proc_root=...), with kill and
# notify injected. The kill path is also exercised end to end on REAL throwaway
# processes that this test spawns itself, with a unique marker path, so no real
# dev session can ever be matched.

import importlib.machinery
import importlib.util
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "reap_idle_dev_sessions.py"
sys.dont_write_bytecode = True
_loader = importlib.machinery.SourceFileLoader("reap_under_test", str(SCRIPT))
_spec = importlib.util.spec_from_loader(_loader.name, _loader)
r = importlib.util.module_from_spec(_spec)
sys.modules[_loader.name] = r      # dataclasses look the module up here
_loader.exec_module(r)

MATCH = "/.claude/remote/ccd-cli/"
HOUR = 3600.0
NOW = 1_800_000_000.0     # fixed "current time" for deterministic tests


# --- fake /proc ---------------------------------------------------------------

class FakeProc:
    """Builds a fake /proc directory tree."""

    def __init__(self, root: Path):
        self.root = root
        root.mkdir(parents=True, exist_ok=True)
        (root / "uptime").write_text("100000.00 200000.00\n")

    def add(self, pid, ppid=1, args=None, cpu_ticks=0, start=1000, cwd=None, comm="x", rss_kb=0, swap_kb=0):
        d = self.root / str(pid)
        d.mkdir(exist_ok=True)
        # fields 3.. : state ppid pgrp session tty tpgid flags minflt cminflt majflt cmajflt utime stime cutime cstime ...
        fields = ["S", str(ppid)] + ["0"] * 9 + [str(cpu_ticks), "0", "0", "0"] + ["0"] * 4 + [str(start)] + ["0"] * 5
        (d / "stat").write_text(f"{pid} ({comm}) " + " ".join(fields) + "\n")
        (d / "cmdline").write_bytes(b"\0".join(a.encode() for a in (args or [])) + b"\0")
        (d / "status").write_text(f"VmRSS:\t{rss_kb} kB\nVmSwap:\t{swap_kb} kB\n")
        if cwd and not (d / "cwd").exists():
            os.symlink(cwd, d / "cwd")
        return d

    def set_cpu(self, pid, ticks):
        d = self.root / str(pid)
        parts = (d / "stat").read_text().rpartition(")")
        f = parts[2].split()
        f[11] = str(ticks)
        (d / "stat").write_text(parts[0] + ")" + " " + " ".join(f) + "\n")

    def remove(self, pid):
        import shutil
        shutil.rmtree(self.root / str(pid))


def session_args(name="2.1.1"):
    return [f"/home/dan/.claude/remote/ccd-cli/{name}", "--output-format", "stream-json"]


@pytest.fixture
def rig(tmp_path):
    proc = FakeProc(tmp_path / "proc")
    projects = tmp_path / "projects"
    projects.mkdir()
    state = tmp_path / "state" / "state.json"
    kills = []
    notes = []

    class Rig:
        fake = proc
        projects_dir = projects
        state_file = state

        def transcript(self, cwd, age_s, name="s.jsonl"):
            d = projects / "".join(c if c.isalnum() else "-" for c in cwd)
            d.mkdir(exist_ok=True)
            f = d / name
            f.write_text("{}")
            os.utime(f, (NOW - age_s, NOW - age_s))

        def run(self, *extra, now=NOW, alive=None):
            fake_kill = lambda pid, sig: kills.append((pid, sig))
            return r.main(
                ["--match", MATCH, "--state-file", str(state), "--projects-dir", str(projects),
                 "--grace", "0", *extra],
                now=now, proc_root=str(proc.root), kill=fake_kill,
                notify=lambda t, x: notes.append((t, x)), sleep=lambda s: None)

        kills_ = kills
        notes_ = notes

    return Rig()


def idle_session(rig, pid=500, cwd="/work/proj", transcript_age=5 * HOUR, **kw):
    rig.fake.add(pid, ppid=1, args=session_args(), cwd=cwd, start=2000 + pid, **kw)
    if transcript_age is not None:
        rig.transcript(cwd, transcript_age)


# --- parsing & discovery ---------------------------------------------------------

def test_stat_parsing_survives_odd_command_names(tmp_path):
    fake = FakeProc(tmp_path / "p")
    fake.add(7, ppid=3, comm="we ) ird (name", cpu_ticks=123, start=777)
    p = r.read_stat(7, str(fake.root))
    assert (p.pid, p.ppid, p.cpu_ticks, p.start_ticks) == (7, 3, 123, 777)


def test_only_argv0_is_matched_not_arguments(rig):
    rig.fake.add(10, args=session_args(), cwd="/a")
    rig.fake.add(11, args=["/bin/bash", "-c", "echo /home/dan/.claude/remote/ccd-cli/2.1.1"], cwd="/a")
    snap = r.snapshot(str(rig.fake.root))
    assert r.find_sessions(snap, MATCH, str(rig.fake.root)) == [10]


def test_nested_session_processes_are_not_separate_sessions(rig):
    rig.fake.add(10, args=session_args(), cwd="/a")
    rig.fake.add(11, ppid=10, args=session_args("child"), cwd="/a")
    snap = r.snapshot(str(rig.fake.root))
    assert r.find_sessions(snap, MATCH, str(rig.fake.root)) == [10]


def test_transcript_dir_is_derived_from_cwd(rig):
    rig.transcript("/mnt/ssd/services/Trevor", 100)
    assert r.transcript_mtime("/mnt/ssd/services/Trevor", rig.projects_dir) == pytest.approx(NOW - 100)
    assert r.transcript_mtime("/somewhere/else", rig.projects_dir) is None
    assert r.transcript_mtime(None, rig.projects_dir) is None


# --- decision ------------------------------------------------------------------------

def test_first_sighting_never_stops_a_session(rig):
    idle_session(rig, transcript_age=10 * HOUR)
    assert rig.run() == []
    assert rig.kills_ == []
    assert json.loads(rig.state_file.read_text())          # baseline recorded


def test_second_sighting_with_old_transcript_and_quiet_cpu_stops_it(rig):
    idle_session(rig, 500, transcript_age=5 * HOUR)
    rig.run(now=NOW)
    res = rig.run(now=NOW + 1800)
    assert [x["pid"] for x in res] == [500]
    assert (500, signal.SIGTERM) in rig.kills_


def test_recent_transcript_keeps_the_session_alive(rig):
    idle_session(rig, 500, transcript_age=1 * HOUR)
    rig.run(now=NOW)
    assert rig.run(now=NOW + 1800) == []


def test_idle_clock_must_reach_the_threshold(rig):
    idle_session(rig, 500, transcript_age=2.2 * HOUR)
    rig.run(now=NOW)
    assert rig.run(now=NOW + 1800) == []            # 2.7 h idle: not yet
    assert len(rig.run(now=NOW + 3600)) == 1        # 3.2 h idle


def test_cpu_activity_resets_the_idle_clock(rig):
    idle_session(rig, 500, transcript_age=10 * HOUR, cpu_ticks=1000)
    rig.run(now=NOW)
    rig.fake.set_cpu(500, 1000 + 100 * 600)         # +600 CPU-seconds in 30 min = 33 % of a core
    assert rig.run(now=NOW + 1800) == []
    assert rig.kills_ == []


def test_idle_keepalive_cpu_does_not_count_as_activity(rig):
    idle_session(rig, 500, transcript_age=10 * HOUR, cpu_ticks=1000)
    rig.run(now=NOW)
    rig.fake.set_cpu(500, 1000 + 100 * 15)          # +15 CPU-seconds in 30 min = 0.8 %
    assert len(rig.run(now=NOW + 1800)) == 1


def test_busy_child_process_protects_the_session(rig):
    idle_session(rig, 500, transcript_age=10 * HOUR)
    rig.fake.add(501, ppid=500, args=["pytest"], cpu_ticks=0, start=9000)
    rig.run(now=NOW)
    rig.fake.set_cpu(501, 100 * 900)                # a test run burning CPU under the session
    assert rig.run(now=NOW + 1800) == []


def test_sessions_without_a_transcript_dir_still_age_out(rig):
    idle_session(rig, 500, transcript_age=None)
    rig.run(now=NOW)
    assert rig.run(now=NOW + 3600) == []            # idle clock only started at first sight
    assert len(rig.run(now=NOW + 3 * HOUR + 1800)) == 1


# --- safety -------------------------------------------------------------------------------

def test_the_callers_own_process_chain_is_never_stopped(rig, monkeypatch):
    idle_session(rig, 500, transcript_age=10 * HOUR)
    rig.fake.add(600, ppid=500, args=["bash"], start=5000)
    rig.fake.add(601, ppid=600, args=["python", "reaper"], start=5001)
    monkeypatch.setattr(r.os, "getpid", lambda: 601)     # pretend we were launched inside session 500
    rig.run(now=NOW)
    assert rig.run(now=NOW + 1800) == []
    assert rig.kills_ == []


def test_pid_reuse_is_not_a_hit(rig):
    idle_session(rig, 500, transcript_age=10 * HOUR)
    rig.run(now=NOW)
    # Same pid, different start time (the old session died and the pid was reused).
    rig.fake.remove(500)
    idle_session(rig, 500, transcript_age=10 * HOUR)
    (rig.fake.root / "500" / "stat").write_text(
        (rig.fake.root / "500" / "stat").read_text().replace("2500", "99999"))
    assert rig.run(now=NOW + 1800) == []                 # looks brand new, so only a first sighting


def test_stop_tree_never_signals_a_pid_that_now_belongs_to_a_different_process(rig):
    """Between identifying a session and signalling it the pid may be reused; the start time catches that."""
    rig.fake.add(500, args=["/bin/unrelated"], start=1234)       # the pid now runs something else
    sent = []
    forced = r.stop_tree([500], {500: 999}, 0, kill=lambda p, s: sent.append((p, s)),
                         sleep=lambda s: None, proc_root=str(rig.fake.root))
    assert sent == [] and forced == []


def test_stop_tree_signals_a_pid_whose_start_time_matches(rig):
    rig.fake.add(500, args=session_args(), start=1234)
    sent = []
    r.stop_tree([500], {500: 1234}, 0, kill=lambda p, s: sent.append((p, s)),
                sleep=lambda s: None, proc_root=str(rig.fake.root))
    assert (500, signal.SIGTERM) in sent


def test_refuses_to_stop_too_many_at_once(rig):
    for i, pid in enumerate((500, 510, 520), 1):
        idle_session(rig, pid, cwd=f"/work/p{i}", transcript_age=10 * HOUR)
    rig.run("--max-reaped", "2", now=NOW)
    assert rig.run("--max-reaped", "2", now=NOW + 1800) == []
    assert rig.kills_ == []


def test_stops_the_whole_tree_and_sigkills_survivors(rig):
    idle_session(rig, 500, transcript_age=10 * HOUR)
    rig.fake.add(501, ppid=500, args=["mcp"], start=9001)
    rig.run(now=NOW)
    res = rig.run(now=NOW + 1800)                        # fake_kill never removes them -> they survive
    termed = {p for p, s in rig.kills_ if s == signal.SIGTERM}
    killed = {p for p, s in rig.kills_ if s == signal.SIGKILL}
    assert termed == {500, 501} and killed == {500, 501}
    assert sorted(res[0]["forced"]) == [500, 501]


# --- dry run & notification --------------------------------------------------------------------

def test_dry_run_kills_nothing_and_reports_each_session_once(rig):
    idle_session(rig, 500, transcript_age=10 * HOUR)
    rig.run("--dry-run", now=NOW)
    first = rig.run("--dry-run", now=NOW + 1800)
    second = rig.run("--dry-run", now=NOW + 3600)
    assert len(first) == 1 and second == []
    assert rig.kills_ == []
    assert len(rig.notes_) == 1
    assert "dry run" in rig.notes_[0][0].lower() and "Would stop" in rig.notes_[0][1]


def test_notification_names_the_session(rig):
    idle_session(rig, 500, cwd="/mnt/ssd/services/Constellation", transcript_age=10 * HOUR,
                 rss_kb=204800, swap_kb=102400)
    rig.run(now=NOW)
    rig.run(now=NOW + 1800)
    (title, text), = rig.notes_
    assert "stopped" in title.lower()
    assert "/mnt/ssd/services/Constellation" in text
    assert "idle 10h" in text and "200 MB RAM" in text and "100 MB swap" in text
    assert "up " in text


def test_one_notification_per_run_even_for_several_sessions(rig):
    for i, pid in enumerate((500, 510), 1):
        idle_session(rig, pid, cwd=f"/work/p{i}", transcript_age=10 * HOUR)
    rig.run(now=NOW)
    rig.run(now=NOW + 1800)
    assert len(rig.notes_) == 1 and rig.notes_[0][1].count("•") == 2


def test_nothing_idle_sends_no_notification(rig):
    idle_session(rig, 500, transcript_age=0.5 * HOUR)
    rig.run(now=NOW)
    rig.run(now=NOW + 1800)
    assert rig.notes_ == []


def test_a_failing_notification_does_not_undo_the_stop(rig, capsys):
    idle_session(rig, 500, transcript_age=10 * HOUR)
    rig.run(now=NOW)
    failing = MagicMock(side_effect=OSError("network down"))
    res = r.main(["--match", MATCH, "--state-file", str(rig.state_file), "--projects-dir",
                  str(rig.projects_dir), "--grace", "0"], now=NOW + 1800, proc_root=str(rig.fake.root),
                 kill=lambda p, s: rig.kills_.append((p, s)), notify=failing, sleep=lambda s: None)
    assert len(res) == 1 and (500, signal.SIGTERM) in rig.kills_
    assert "notification failed" in capsys.readouterr().out


def test_stopped_sessions_leave_the_state_file(rig):
    idle_session(rig, 500, transcript_age=10 * HOUR)
    rig.run(now=NOW)
    rig.run(now=NOW + 1800)
    assert json.loads(rig.state_file.read_text()) == {}


def test_state_for_vanished_sessions_is_pruned(rig):
    idle_session(rig, 500, transcript_age=0.1 * HOUR)
    rig.run(now=NOW)
    assert len(json.loads(rig.state_file.read_text())) == 1
    rig.fake.remove(500)
    rig.run(now=NOW + 1800)
    assert json.loads(rig.state_file.read_text()) == {}


def test_env_file_variable_lookup(tmp_path):
    f = tmp_path / ".env"
    f.write_text('# comment\nOTHER=1\nHOOK="https://example.invalid/x"\n')
    assert r.read_env_var(str(f), "HOOK") == "https://example.invalid/x"
    assert r.read_env_var(str(f), "MISSING") is None
    assert r.read_env_var(str(tmp_path / "nope"), "HOOK") is None


# --- end to end on real processes ------------------------------------------------------------------

@pytest.fixture
def real_session(tmp_path):
    """A real throwaway 'session' (argv[0] under a unique marker path) with one real child."""
    marker = f"/{tmp_path.name}/.claude/remote/ccd-cli/"
    fake_bin = tmp_path / "x"
    fake_bin.mkdir()
    code = ("import subprocess, time; subprocess.Popen(['sleep', '300']); time.sleep(300)")
    proc = subprocess.Popen([marker + "fake-session", "-c", code], executable=sys.executable,
                            cwd=str(fake_bin), start_new_session=True)
    time.sleep(0.5)
    yield marker, proc
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    proc.wait()


def test_real_processes_are_stopped_and_the_unrelated_ones_survive(real_session, tmp_path):
    marker, proc = real_session
    bystander = subprocess.Popen(["sleep", "300"])
    try:
        snap = r.snapshot()
        assert r.find_sessions(snap, marker) == [proc.pid]
        assert bystander.pid not in r.find_sessions(snap, marker)

        state = tmp_path / "state.json"
        key = f"{proc.pid}:{snap[proc.pid].start_ticks}"
        long_ago = time.time() - 10 * HOUR
        state.write_text(json.dumps({key: {
            "first_seen": long_ago, "checked_at": time.time() - 1800, "last_total": snap[proc.pid].cpu_ticks,
            "last_active": long_ago, "observations": 2}}))
        notes = []
        res = r.main(["--match", marker, "--state-file", str(state), "--projects-dir", str(tmp_path / "none"),
                      "--grace", "5"], notify=lambda t, x: notes.append((t, x)))

        assert [x["pid"] for x in res] == [proc.pid]
        assert res[0]["procs"] == 2                         # the session and its sleeping child
        proc.wait(timeout=10)                               # reaped: the session really exited
        assert proc.returncode == -signal.SIGTERM
        assert bystander.poll() is None                     # unrelated process untouched
        assert len(notes) == 1 and "stopped" in notes[0][0].lower()
        time.sleep(0.2)
        assert not any(p.ppid == proc.pid for p in r.snapshot().values())   # the child is gone too
    finally:
        bystander.kill()
        bystander.wait()
