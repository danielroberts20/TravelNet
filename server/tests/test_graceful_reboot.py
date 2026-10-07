# tests/test_graceful_reboot.py
#
# End-to-end tests of server/scripts/graceful_reboot.sh with stubbed curl / docker / sudo.
# The stubs shadow the real commands via PATH and only log their arguments, so
# nothing is stopped, notified or rebooted. Every test asserts the stubs are
# the ones the script will resolve before running it.

import os
import shutil
import subprocess
import time
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "graceful_reboot.sh"

# The script is gitignored (site-specific), so it is absent on a fresh checkout / in CI.
if not SCRIPT.exists():
    pytest.skip(f"{SCRIPT} is not in this checkout (gitignored)", allow_module_level=True)

CURL_STUB = """#!/bin/bash
echo "curl $*" >> "$STUB_LOG"
case "$*" in *"-K -"*) cat >> "$STUB_LOG.stdin";; esac    # credentials arrive as a config line on stdin
case "$*" in
  *flow_runs/count*)
    [ "$COUNT_MODE" = "fail" ] && exit 7
    if [ -s "$COUNT_FILE" ]; then
      n=$(head -n1 "$COUNT_FILE")
      [ "$(wc -l < "$COUNT_FILE")" -gt 1 ] && sed -i 1d "$COUNT_FILE"
      printf '%s' "$n"
    fi
    ;;
esac
exit 0
"""
LOG_STUB = '#!/bin/bash\necho "{name} $*" >> "$STUB_LOG"\nexit 0\n'


@pytest.fixture
def rig(tmp_path):
    bindir = tmp_path / "bin"
    bindir.mkdir()
    (bindir / "curl").write_text(CURL_STUB)
    for name in ("docker", "sudo"):
        (bindir / name).write_text(LOG_STUB.format(name=name))
    for f in bindir.iterdir():
        f.chmod(0o755)

    env_file = tmp_path / ".env"
    env_file.write_text("CUSTOM_NOTIFICATION_TIME_SENSITIVE=http://stub.invalid/hook\n")
    compose_dir = tmp_path / "compose"
    compose_dir.mkdir()
    log = tmp_path / "calls.log"
    log.write_text("")
    counts = tmp_path / "counts"
    counts.write_text("")

    env = {
        "PATH": f"{bindir}:/usr/bin:/bin",
        "STUB_LOG": str(log), "COUNT_FILE": str(counts), "COUNT_MODE": "",
        "TRAVELNET_ENV_FILE": str(env_file), "TRAVELNET_COMPOSE_DIR": str(compose_dir),
        "FLOW_WAIT_MAX_S": "30", "FLOW_WAIT_POLL_S": "0", "REBOOT_DELAY_S": "0",
        "HOME": str(tmp_path),
    }

    class Rig:
        def add_env(self, line):
            with open(env_file, "a") as f:
                f.write(line + "\n")

        def stdin(self):
            p = Path(str(log) + ".stdin")
            return p.read_text() if p.exists() else ""

        def run(self, reason="scheduled", counts_seq=None, **extra_env):
            if counts_seq is not None:
                counts.write_text("".join(f"{c}\n" for c in counts_seq))
            e = {**env, **{k: str(v) for k, v in extra_env.items()}}
            # Safety: the script must resolve sudo/docker/curl to the stubs, never the real ones.
            for cmd in ("sudo", "docker", "curl"):
                found = subprocess.run(["bash", "-c", f"command -v {cmd}"], env=e,
                                       capture_output=True, text=True).stdout.strip()
                assert found == str(bindir / cmd), f"{cmd} resolves to {found}, not the stub"
            result = subprocess.run(["bash", str(SCRIPT), reason], env=e, cwd=tmp_path,
                                    capture_output=True, text=True, timeout=60)
            time.sleep(0.2)   # the stubbed `sudo ... reboot` is started in the background
            return result

        def calls(self):
            return [ln for ln in log.read_text().splitlines() if ln.strip()]

    return Rig()


def kinds(calls):
    """Reduce the call log to the sequence of interesting events."""
    out = []
    for c in calls:
        if "flow_runs/count" in c:
            out.append("count")
        elif c.startswith("docker compose stop"):
            out.append("stop")
        elif "/maintenance" in c:
            out.append("maintenance")
        elif c.startswith("sudo") and "/sbin/reboot" in c:
            out.append("reboot")
        elif "stub.invalid/hook" in c:
            out.append("notify")
    return out


def test_script_has_valid_bash_syntax():
    assert subprocess.run(["bash", "-n", str(SCRIPT)]).returncode == 0


def test_waits_until_flow_runs_finish_then_stops_and_reboots(rig):
    r = rig.run("scheduled", counts_seq=[2, 1, 0])
    assert r.returncode == 0, r.stderr
    assert kinds(rig.calls()) == ["notify", "count", "count", "count", "stop", "maintenance", "reboot"]
    assert "Waiting for 2 flow run(s)" in r.stdout and "No flow runs in flight" in r.stdout


def test_nothing_in_flight_proceeds_immediately(rig):
    r = rig.run("scheduled", counts_seq=[0])
    assert kinds(rig.calls()) == ["notify", "count", "stop", "maintenance", "reboot"]
    assert "Waiting" not in r.stdout


def test_count_request_asks_for_running_and_pending(rig):
    rig.run("manual", counts_seq=[0])
    (req,) = [c for c in rig.calls() if "flow_runs/count" in c]
    assert "RUNNING" in req and "PENDING" in req and "localhost:4200/api" in req


def test_watchdog_reboot_never_waits_or_queries_prefect(rig):
    r = rig.run("watchdog", counts_seq=[5, 5, 5])
    assert kinds(rig.calls()) == ["notify", "stop", "maintenance", "reboot"]
    assert "not waiting" in r.stdout


def test_unreachable_prefect_does_not_block_the_reboot(rig):
    r = rig.run("scheduled", COUNT_MODE="fail")
    assert r.returncode == 0
    assert kinds(rig.calls()) == ["notify", "count", "stop", "maintenance", "reboot"]
    assert "Prefect API not answering" in r.stdout


def test_garbage_response_does_not_block_the_reboot(rig):
    rig.run("scheduled", counts_seq=['{"detail":"Not Found"}'])
    assert kinds(rig.calls()) == ["notify", "count", "stop", "maintenance", "reboot"]


def test_gives_up_waiting_after_the_limit_but_still_reboots(rig):
    t0 = time.time()
    # Bash's $SECONDS has 1 s granularity, so a limit of N gives up after N-1..N seconds.
    # Use 2 s with a 0.2 s poll so there are always several polls, even on a loaded machine.
    r = rig.run("scheduled", counts_seq=[3], FLOW_WAIT_MAX_S=2, FLOW_WAIT_POLL_S="0.2")
    assert r.returncode == 0
    ev = kinds(rig.calls())
    assert ev.count("count") >= 2                       # it polled repeatedly, not just once
    assert ev[-3:] == ["stop", "maintenance", "reboot"]
    assert "Gave up after 2s with 3 flow run(s)" in r.stdout
    assert time.time() - t0 < 20


@pytest.mark.parametrize("reason,phrase", [
    ("scheduled", "scheduled monthly reboot"),
    ("watchdog", "triggered by watchdog"),
    ("manual", "(manual)"),
    ("anything-else", "(manual)"),
])
def test_notification_text_matches_the_reason(rig, reason, phrase):
    rig.run(reason, counts_seq=[0])
    (notify,) = [c for c in rig.calls() if "stub.invalid/hook" in c]
    assert phrase in notify


def test_compose_stop_runs_in_the_compose_directory(rig, tmp_path):
    # The docker stub logs only its args; check the script cd'd by having it write its cwd.
    stub = tmp_path / "bin" / "docker"
    stub.write_text('#!/bin/bash\necho "docker $* cwd=$PWD" >> "$STUB_LOG"\n')
    stub.chmod(0o755)
    rig.run("manual", counts_seq=[0])
    (stop,) = [c for c in rig.calls() if c.startswith("docker compose stop")]
    assert stop.endswith(f"cwd={tmp_path / 'compose'}")


def test_missing_env_file_fails_before_anything_is_stopped(rig, tmp_path):
    r = rig.run("scheduled", TRAVELNET_ENV_FILE=str(tmp_path / "nope.env"))
    assert r.returncode != 0
    assert kinds(rig.calls()) == []


def test_prefect_credentials_reach_curl_on_stdin_never_in_arguments(rig):
    rig.add_env("PREFECT_API_AUTH_STRING=svc:s3cr3t-value")
    rig.run("manual", counts_seq=[0])
    assert not any("s3cr3t-value" in c for c in rig.calls())          # not in any argument (ps would show it)
    assert 'user = "svc:s3cr3t-value"' in rig.stdin()
    (req,) = [c for c in rig.calls() if "flow_runs/count" in c]
    assert "-K -" in req


def test_without_credentials_curl_gets_no_user_line(rig):
    rig.run("manual", counts_seq=[0])
    assert "user =" not in rig.stdin()
