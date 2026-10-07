# tests/test_health_script.py
#
# Tests for the host health monitor (scripts/check_system_health.py).
# Until the reviewed draft is copied over the live script, the tests load the
# draft; afterwards they load the live script. Whichever has the new checks wins.

import importlib.machinery
import importlib.util
import json
import time
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

ROOT = Path(__file__).resolve().parents[2]
CANDIDATES = [
    ROOT / "scripts" / "check_system_health.py",
    ROOT / "docs" / "drafts" / "check_system_health.py.proposed",
]


def _load():
    for path in CANDIDATES:
        if path.exists() and "def evaluate_containers" in path.read_text():
            loader = importlib.machinery.SourceFileLoader("check_system_health_under_test", str(path))
            spec = importlib.util.spec_from_loader(loader.name, loader)
            mod = importlib.util.module_from_spec(spec)
            loader.exec_module(mod)
            return mod
    pytest.skip("no version of check_system_health.py with the new checks found")


h = _load()


def info(name, health=None, restarts=0, cid="abc123def4567890", status="running"):
    state = {"Status": status}
    if health:
        state["Health"] = {"Status": health}
    return {"Name": f"/{name}", "Id": cid, "RestartCount": restarts, "State": state}


# --- container health / restarts / cgroup OOM ---

def test_healthy_container_raises_no_alert():
    assert h.evaluate_containers([info("travelnet", "healthy")], {}, read_events=lambda c: {}) == []


def test_container_without_healthcheck_is_not_flagged():
    assert h.evaluate_containers([info("trevor")], {}, read_events=lambda c: {}) == []


def test_unhealthy_container_is_critical_and_named():
    (alert,) = h.evaluate_containers([info("travelnet", "unhealthy")], {}, read_events=lambda c: {})
    assert alert.critical and alert.cooldown_key == "unhealthy"
    assert alert.metadata == {"name": "travelnet"}


def test_starting_container_is_not_unhealthy():
    assert h.evaluate_containers([info("travelnet", "starting")], {}, read_events=lambda c: {}) == []


def test_restart_count_increase_alerts_but_first_observation_does_not():
    state = {}
    assert h.evaluate_containers([info("worker", restarts=2)], state, read_events=lambda c: {}) == []
    assert h.evaluate_containers([info("worker", restarts=2)], state, read_events=lambda c: {}) == []
    (alert,) = h.evaluate_containers([info("worker", restarts=5)], state, read_events=lambda c: {})
    assert alert.cooldown_key == "restartloop" and "3 time(s)" in alert.body


def test_recreated_container_gets_a_fresh_baseline():
    state = {}
    h.evaluate_containers([info("worker", restarts=9, cid="aaaaaaaaaaaa1")], state, read_events=lambda c: {})
    # Same name, new container id, restart count back to 0: no alert, old baseline dropped.
    assert h.evaluate_containers([info("worker", restarts=0, cid="bbbbbbbbbbbb2")], state,
                                 read_events=lambda c: {}) == []
    assert list(state["restart_counts"]) == ["worker:bbbbbbbbbbbb"]


def test_cgroup_oom_kill_increase_is_critical():
    state = {}
    h.evaluate_containers([info("worker")], state, read_events=lambda c: {"oom_kill": 0, "max": 5})
    assert h.evaluate_containers([info("worker")], state, read_events=lambda c: {"oom_kill": 0, "max": 99}) == []
    (alert,) = h.evaluate_containers([info("worker")], state, read_events=lambda c: {"oom_kill": 2})
    assert alert.critical and alert.cooldown_key == "cgroup_oom"


# --- swap-in rate ---

def test_swapin_rate_needs_a_baseline_and_ignores_counter_resets():
    assert h.swapin_rate(None, 1000, 50) is None
    assert h.swapin_rate({"t": 990, "v": 10}, 1000, 50) is None        # too soon (<60 s)
    assert h.swapin_rate({"t": 0, "v": 1_000_000}, 900, 5) is None     # counter went backwards (reboot)


def test_swapin_rate_is_pages_per_second():
    assert h.swapin_rate({"t": 0, "v": 1000}, 900, 10_000) == pytest.approx(10.0)


def test_swap_io_alert_only_above_threshold(monkeypatch):
    now = time.time()
    monkeypatch.setattr(h, "STATE", {"pswpin": {"t": now - 900, "v": 0}})
    quiet = h.THRESHOLDS["swapin_pages_per_s"] * 900 * 0.5
    noisy = h.THRESHOLDS["swapin_pages_per_s"] * 900 * 2
    with patch.object(h.Path, "read_text", return_value=f"pswpin {int(quiet)}\n"):
        assert h.check_swap_io() == []
    monkeypatch.setattr(h, "STATE", {"pswpin": {"t": now - 900, "v": 0}})
    with patch.object(h.Path, "read_text", return_value=f"pswpin {int(noisy)}\n"):
        (alert,) = h.check_swap_io()
    assert alert.cooldown_key == "swapio" and not alert.critical


def test_plain_high_swap_percentage_no_longer_alerts():
    meminfo = "MemTotal: 3800000 kB\nMemAvailable: 2000000 kB\nSwapTotal: 2000000 kB\nSwapFree: 700000 kB\n"
    with patch.object(h.Path, "read_text", return_value=meminfo):
        assert h.check_memory() == []                                  # 65 % swap used: normal with zram


def test_nearly_full_swap_is_critical():
    meminfo = "MemTotal: 3800000 kB\nMemAvailable: 2000000 kB\nSwapTotal: 2000000 kB\nSwapFree: 100000 kB\n"
    with patch.object(h.Path, "read_text", return_value=meminfo):
        (alert,) = h.check_memory()
    assert alert.critical and alert.key == "swap"


# --- restart budget ---

def test_restart_budget_allows_three_then_refuses_and_expires():
    state, t0 = {}, 1_000_000.0
    run = MagicMock(return_value=MagicMock(returncode=0, stderr=""))
    with patch.object(h.subprocess, "run", run):
        results = [h.restart_with_budget("travelnet", ["docker", "restart", "travelnet"], state, t0 + i)[0]
                   for i in range(4)]
        assert results == [True, True, True, False]
        assert run.call_count == 3                                       # the 4th never ran
        later = t0 + h.THRESHOLDS["restart_window_h"] * 3600 + 10
        assert h.restart_with_budget("travelnet", ["docker", "restart", "travelnet"], state, later)[0] is True


def test_failed_restart_is_reported_and_still_counts():
    state = {}
    fail = MagicMock(return_value=MagicMock(returncode=1, stderr="no such container"))
    with patch.object(h.subprocess, "run", fail):
        ok, msg = h.restart_with_budget("x", ["docker", "restart", "x"], state, 1.0)
    assert ok is False and "no such container" in msg and len(state["restarts"]["x"]) == 1


def test_unhealthy_prefect_server_restarts_the_systemd_unit_not_docker():
    with patch.object(h, "restart_with_budget", return_value=(True, "ok")) as r:
        h.mitigate_unhealthy("prefect-server")
        h.mitigate_unhealthy("travelnet")
    assert r.call_args_list[0].args[1] == ["systemctl", "restart", "prefect-server.service"]
    assert r.call_args_list[1].args[1] == ["docker", "restart", "travelnet"]


def test_services_never_restart_docker_itself():
    with patch.object(h, "restart_with_budget") as r:
        ok, msg = h.mitigate_service("docker")
    assert ok is False and not r.called


# --- container-down detection and mitigation ---

def _run_result(stdout="", returncode=0, stderr=""):
    return MagicMock(stdout=stdout, returncode=returncode, stderr=stderr)


def test_container_down_uses_exact_names_not_substrings():
    # Only "travelnet-nginx" is up: the old substring check wrongly reported "travelnet" as running.
    ps = _run_result("travelnet-nginx\n")
    with patch.object(h, "EXPECTED_CONTAINERS", ["travelnet", "travelnet-nginx"]), \
         patch.object(h.subprocess, "run", return_value=ps):
        alerts = h.check_containers()
    assert [a.metadata["name"] for a in alerts] == ["travelnet"]


def test_mitigate_prefers_exact_name_over_substring_match():
    calls = []

    def fake_run(cmd, **kw):
        calls.append(cmd)
        if cmd[:2] == ["docker", "ps"]:
            return _run_result("travelnet-nginx\ntravelnet\n")
        return _run_result()

    with patch.object(h.subprocess, "run", side_effect=fake_run):
        ok, _ = h.mitigate_container("travelnet")
    assert ok and ["docker", "start", "travelnet"] in calls
    assert ["docker", "start", "travelnet-nginx"] not in calls


def test_mitigate_prefect_server_goes_through_systemd():
    with patch.object(h.subprocess, "run", return_value=_run_result("prefect-server\n")), \
         patch.object(h, "restart_with_budget", return_value=(True, "restarted")) as r:
        h.mitigate_container("prefect-server")
    assert r.call_args.args[1] == ["systemctl", "restart", "prefect-server.service"]


# --- services, mounts, backups, prefect db ---

def test_inactive_service_alerts():
    with patch.object(h.subprocess, "run", side_effect=[_run_result("active\n"), _run_result("failed\n"),
                                                         _run_result("active\n")]):
        alerts = h.check_services()
    assert [a.metadata["unit"] for a in alerts] == ["cloudflared"]


def test_unmounted_filesystem_is_critical():
    with patch.object(h.os.path, "ismount", side_effect=lambda m: m != "/mnt/ssd"):
        (alert,) = h.check_mounts()
    assert alert.critical and "/mnt/ssd" in alert.title


def test_stale_and_missing_backups(tmp_path):
    with patch.object(h, "BACKUP_HOST_DIR", tmp_path):
        (missing,) = h.check_backups()
        assert missing.critical
        f = tmp_path / "2026-10-01_14-00-00.db.zst"
        f.write_text("x")
        old = time.time() - 50 * 3600
        import os
        os.utime(f, (old, old))
        (stale,) = h.check_backups()
        assert "50 h old" in stale.body and not stale.critical
        os.utime(f, None)
        assert h.check_backups() == []


def test_prefect_db_size_threshold(tmp_path):
    db = tmp_path / "prefect.db"
    db.write_bytes(b"0")
    with patch.object(h, "PREFECT_DB_FILE", db):
        assert h.check_prefect_db() == []
    big = MagicMock(st_size=int(2e9))
    with patch.object(h, "PREFECT_DB_FILE", MagicMock(stat=MagicMock(return_value=big))):
        (alert,) = h.check_prefect_db()
    assert alert.cooldown_key == "prefectdb"


# --- state persistence and dry run ---

def test_state_round_trips_and_survives_garbage(tmp_path):
    f = tmp_path / "state.json"
    with patch.object(h, "STATE_FILE", f):
        h.save_state({"a": 1})
        assert h.load_state() == {"a": 1}
        f.write_text("not json{")
        assert h.load_state() == {}
        f.unlink()
        assert h.load_state() == {}


def test_dry_run_has_no_side_effects(tmp_path):
    from_h = h.Alert("x", "T", "B", "unhealthy", critical=True, metadata={"name": "travelnet"})
    state_file = tmp_path / "state.json"
    with patch.object(h, "STATE_FILE", state_file), \
         patch.object(h, "COOLDOWN_DIR", tmp_path / "cooldowns"), \
         patch.object(h, "_read_env", return_value="https://example.invalid/hook"), \
         patch.object(h, "_send") as send, \
         patch.object(h, "mitigate_unhealthy") as mitigate, \
         patch.object(h, "_mark_cooldown") as mark, \
         patch.object(h, "check_mounts", return_value=[from_h]), \
         patch.multiple(h, **{n: MagicMock(return_value=[]) for n in (
             "check_cpu_temp", "check_disk", "check_memory", "check_swap_io", "check_load",
             "check_containers", "check_container_health", "check_services", "check_wal",
             "check_oom", "check_smart", "check_zombies", "check_backups", "check_prefect_db")}):
        h.main(["--dry-run"])
    send.assert_not_called()
    mitigate.assert_not_called()
    mark.assert_not_called()
    assert not state_file.exists()
    assert not (tmp_path / "cooldowns").exists()


def test_live_run_mitigates_notifies_and_saves_state(tmp_path):
    alert = h.Alert("unhealthy_travelnet", "T", "B", "unhealthy", critical=True, metadata={"name": "travelnet"})
    state_file = tmp_path / "state.json"
    with patch.object(h, "STATE_FILE", state_file), \
         patch.object(h, "COOLDOWN_DIR", tmp_path / "cooldowns"), \
         patch.object(h, "_read_env", return_value="https://example.invalid/hook"), \
         patch.object(h, "_send") as send, \
         patch.object(h, "mitigate_unhealthy", return_value=(True, "restarted")) as mitigate, \
         patch.object(h, "check_mounts", return_value=[alert]), \
         patch.multiple(h, **{n: MagicMock(return_value=[]) for n in (
             "check_cpu_temp", "check_disk", "check_memory", "check_swap_io", "check_load",
             "check_containers", "check_container_health", "check_services", "check_wal",
             "check_oom", "check_smart", "check_zombies", "check_backups", "check_prefect_db")}):
        h.main([])
    mitigate.assert_called_once_with("travelnet")
    assert "restarted" in send.call_args.args[2]
    assert state_file.exists()
