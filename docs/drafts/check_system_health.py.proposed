#!/usr/bin/env python3
"""
check_system_health.py
Host-level system health monitor for the TravelNet Raspberry Pi.

Reads the Pushcut warning webhook URL from the TravelNet .env file.
File-based cooldowns in /tmp/travelnet_health/ prevent notification spam.
SMART checks are throttled to once per 24 h regardless of cron frequency.

Checks:
  • CPU temperature        — warn ≥ 70 °C, critical ≥ 80 °C (Pi 4B throttle point)
  • Disk usage             — warn ≥ 80 %, critical ≥ 90 % (SSD, HDD, root)
  • RAM usage              — warn ≥ 85 %, critical ≥ 95 %
  • Swap                   — critical ≥ 90 % used; warn on sustained swap-IN rate (real thrashing,
                             not just cold pages parked in zram)
  • CPU load average       — warn ≥ 4.0 (Pi 4B has 4 cores)
  • Docker containers      — alert if any expected container is not running (exact name match),
                             is failing its Docker healthcheck, has restarted since the last run,
                             or had a cgroup OOM kill
  • systemd services       — docker, cloudflared, prefect-server must be active
  • Mounts                 — /mnt/ssd and /mnt/linux must be mounted (everything lives there)
  • DB backups             — newest backup must be < 36 h old
  • Prefect DB size        — warn ≥ 1.5 GB (retention should keep it well below)
  • SQLite WAL file size   — warn ≥ 100 MB (stuck transaction / checkpoint failure)
  • OOM kill events        — alert if kernel killed a process in the last hour
  • SMART disk health      — FAILED overall + reallocated/pending sectors (once/day)
  • Zombie processes       — warn if ≥ 5 zombies (Docker/subprocess leak)

Mitigations (run automatically on actionable alerts):
  • Disk critical          — docker builder prune, prune old backups (keep 3), truncate health log
  • Container down         — docker start <container> (systemctl restart for prefect-server)
  • Container unhealthy    — docker restart <container>, at most 3 restarts per 6 h per container
  • Service down           — systemctl restart for cloudflared / prefect-server (same budget)
  • WAL large              — PRAGMA wal_checkpoint(TRUNCATE), escalate if still large
  • SMART failure          — emergency backup to healthy drive + R2
"""

import json
import os
import shutil
import subprocess
import sys
import time
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────

ENV_FILE        = Path("/home/dan/services/TravelNet/server/.env")
COOLDOWN_DIR    = Path("/tmp/travelnet_health")
WAL_FILE        = Path("/mnt/ssd/docker/services/travelnet/data/travel.db-wal")
DB_HOST_PATH    = Path("/mnt/ssd/docker/services/travelnet/data/travel.db")
BACKUP_HOST_DIR = Path("/mnt/linux/docker/services/travelnet/data/backups/db")
HEALTH_LOG      = Path("/home/dan/services/TravelNet/logs/health_monitor.log")
PREFECT_DB_FILE = Path("/mnt/ssd/docker/services/prefect/data/prefect.db")
# Unlike COOLDOWN_DIR (/tmp, cleared at reboot) this survives reboots, so counters
# such as restart counts and pswpin are not mistaken for jumps after the monthly reboot.
STATE_FILE      = Path("/var/tmp/travelnet_health_state.json")

# Filesystems the whole stack depends on (root is covered by the disk check).
REQUIRED_MOUNTS = ["/mnt/ssd", "/mnt/linux"]

MOUNTS = [
    ("/mnt/ssd",   "SSD"),
    ("/mnt/linux", "HDD"),
    ("/",          "root"),
]

EXPECTED_CONTAINERS = [
    "server-prefect-worker-1",
    "travelnet-nginx",
    "travelnet-dashboard",
    "travelnet",
    "prefect-server",
    "trevor",
    "constellation",
]

# Containers whose lifecycle belongs to a systemd unit, not docker/compose.
# `docker start` does not work for these (the unit removes the container on stop).
SYSTEMD_MANAGED = {"prefect-server": "prefect-server.service"}

# systemd units that must be active.
EXPECTED_SERVICES = ["docker", "cloudflared", "prefect-server"]
# Units we will try to restart (never docker itself).
RESTARTABLE_SERVICES = {"cloudflared", "prefect-server"}

# (device_path, label, smartctl_type)
SMART_DEVICES = [
    ("/dev/sda", "SSD", "sat"),   # Samsung 870 EVO 500GB, ASM225CM bridge
    ("/dev/sdb", "HDD", "auto"),  # WD 6TB
]

# SMART emergency backup
SSD_DEVICE      = "/dev/sda"
HDD_DEVICE      = "/dev/sdb"
SD_MARGIN_BYTES = 2 * 1024 * 1024 * 1024  # 2 GB

EMERGENCY_BACKUP_DIRS = {
    "ssd": Path("/mnt/ssd/emergency_backups"),
    "hdd": Path("/mnt/linux/emergency_backups"),
    "sd":  Path("/home/dan/emergency_backups"),
}

# ── Thresholds ────────────────────────────────────────────────────────────────

THRESHOLDS = {
    "cpu_temp_warn_c":  70.0,
    "cpu_temp_crit_c":  80.0,
    "disk_warn_pct":    80,
    "disk_crit_pct":    90,
    "ram_warn_pct":     85,
    "ram_crit_pct":     95,
    "swap_crit_pct":    90,     # exhaustion risk; plain "swap in use" is normal with zram
    "swapin_pages_per_s": 100,  # ~0.4 MB/s of swap-in, sustained between runs = thrashing
    "backup_max_age_h": 36,
    "prefect_db_warn_gb": 1.5,
    "restart_budget":   3,      # automatic restarts per container/service ...
    "restart_window_h": 6,      # ... per this many hours
    "load_warn":        4.0,
    "wal_warn_mb":      100,
    "zombie_warn":      5,
}

COOLDOWNS = {
    "cpu_temp":   3_600,   # 1 h
    "disk":      21_600,   # 6 h per mount
    "ram":        7_200,   # 2 h
    "swap":       7_200,
    "swapio":     7_200,
    "unhealthy": 1_800,   # 30 min
    "restartloop": 3_600,
    "service":   1_800,
    "mount":     3_600,
    "backup":   21_600,   # 6 h
    "prefectdb": 86_400,
    "cgroup_oom": 43_200,
    "load":       3_600,
    "container":  1_800,   # 30 min — critical
    "wal":        3_600,
    "oom":       43_200,   # 12 h
    "smart":     86_400,   # 24 h (also throttles the check itself)
    "zombie":     7_200,
}

# ── Data ──────────────────────────────────────────────────────────────────────

@dataclass
class Alert:
    key: str
    title: str
    body: str
    cooldown_key: str
    critical: bool = False
    metadata: dict = field(default_factory=dict)

# ── Helpers ───────────────────────────────────────────────────────────────────

def _read_env(key: str) -> str | None:
    try:
        for line in ENV_FILE.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            if k.strip() == key:
                return v.strip().strip('"').strip("'")
    except Exception:
        pass
    return None


def _send(webhook_url: str, title: str, body: str) -> None:
    payload = json.dumps({"title": title, "text": body}).encode()
    req = urllib.request.Request(
        webhook_url, data=payload,
        headers={"Content-Type": "application/json"}, method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            print(f"  → sent ({resp.status})")
    except Exception as e:
        print(f"  → Pushcut failed: {e}")


def _cooldown_path(key: str) -> Path:
    return COOLDOWN_DIR / f"alert_{key.replace('/', '_')}"


def _in_cooldown(key: str, cooldown_key: str) -> bool:
    p = _cooldown_path(key)
    if not p.exists():
        return False
    age_s = datetime.now(timezone.utc).timestamp() - p.stat().st_mtime
    return age_s < COOLDOWNS[cooldown_key]


def _mark_cooldown(key: str) -> None:
    COOLDOWN_DIR.mkdir(exist_ok=True)
    _cooldown_path(key).touch()


def _last_run_within(key: str, seconds: int) -> bool:
    p = COOLDOWN_DIR / f"lastrun_{key}"
    if not p.exists():
        return False
    return (datetime.now(timezone.utc).timestamp() - p.stat().st_mtime) < seconds


def _mark_last_run(key: str) -> None:
    COOLDOWN_DIR.mkdir(exist_ok=True)
    (COOLDOWN_DIR / f"lastrun_{key}").touch()

# ── Persistent state (restart counters, swap-in baseline, restart budget) ─────

STATE: dict = {}


def load_state() -> dict:
    try:
        return json.loads(STATE_FILE.read_text())
    except Exception:
        return {}


def save_state(state: dict) -> None:
    try:
        tmp = STATE_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(state))
        tmp.replace(STATE_FILE)
    except Exception as e:
        print(f"  state: save failed — {e}")


def swapin_rate(prev: dict | None, now_t: float, now_v: int) -> float | None:
    """Pages/s swapped in since the previous run, or None if it cannot be computed
    (first run, runs too close together, or the counter reset at reboot)."""
    if not prev:
        return None
    dt = now_t - prev["t"]
    if dt < 60 or now_v < prev["v"]:
        return None
    return (now_v - prev["v"]) / dt


def restart_allowed(state: dict, key: str, now: float) -> bool:
    """True if `key` has used fewer than the restart budget within the window."""
    window_s = THRESHOLDS["restart_window_h"] * 3600
    stamps = [t for t in state.setdefault("restarts", {}).get(key, []) if now - t < window_s]
    state["restarts"][key] = stamps
    return len(stamps) < THRESHOLDS["restart_budget"]


def restart_with_budget(key: str, cmd: list[str], state: dict | None = None,
                        now: float | None = None) -> tuple[bool, str]:
    """Run a restart command unless this key has exhausted its budget (stops restart loops)."""
    state = STATE if state is None else state
    now = time.time() if now is None else now
    if not restart_allowed(state, key, now):
        return False, (f"restart budget exhausted ({THRESHOLDS['restart_budget']} in "
                       f"{THRESHOLDS['restart_window_h']} h) — needs manual attention")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    state["restarts"][key].append(now)
    if result.returncode == 0:
        return True, f"restarted via `{' '.join(cmd)}`"
    return False, f"restart failed: {result.stderr.strip()[:100]}"

# ── Checks ────────────────────────────────────────────────────────────────────

def check_cpu_temp() -> list[Alert]:
    try:
        temp_c = int(Path("/sys/class/thermal/thermal_zone0/temp").read_text()) / 1000
    except Exception as e:
        print(f"  cpu_temp: read failed — {e}")
        return []

    print(f"  cpu_temp: {temp_c:.1f} °C")

    if temp_c >= THRESHOLDS["cpu_temp_crit_c"]:
        return [Alert("cpu_temp", "🌡️ Pi CPU Critical Temp",
                      f"CPU at {temp_c:.0f} °C — throttling imminent", "cpu_temp", critical=True)]
    if temp_c >= THRESHOLDS["cpu_temp_warn_c"]:
        return [Alert("cpu_temp", "🌡️ Pi CPU High Temp",
                      f"CPU at {temp_c:.0f} °C (warn threshold {THRESHOLDS['cpu_temp_warn_c']:.0f} °C)", "cpu_temp")]
    return []


def check_disk() -> list[Alert]:
    alerts = []
    for mount, label in MOUNTS:
        try:
            usage = shutil.disk_usage(mount)
        except Exception as e:
            print(f"  disk {label}: read failed — {e}")
            continue
        pct     = usage.used / usage.total * 100
        free_gb = usage.free / 1e9
        print(f"  disk {label} ({mount}): {pct:.1f}% used, {free_gb:.1f} GB free")
        key = f"disk_{mount}"
        if pct >= THRESHOLDS["disk_crit_pct"]:
            alerts.append(Alert(
                key, f"💾 Disk Critical — {label}",
                f"{mount} is {pct:.0f}% full ({free_gb:.1f} GB free)",
                "disk", critical=True,
                metadata={"mount": mount, "label": label},
            ))
        elif pct >= THRESHOLDS["disk_warn_pct"]:
            alerts.append(Alert(
                key, f"💾 Disk Warning — {label}",
                f"{mount} is {pct:.0f}% full ({free_gb:.1f} GB free)",
                "disk",
                metadata={"mount": mount, "label": label},
            ))
    return alerts


def check_memory() -> list[Alert]:
    alerts = []
    try:
        info = {}
        for line in Path("/proc/meminfo").read_text().splitlines():
            k, _, v = line.partition(":")
            info[k.strip()] = int(v.strip().split()[0])

        total     = info["MemTotal"]
        available = info["MemAvailable"]
        used_pct  = (total - available) / total * 100

        swap_total = info.get("SwapTotal", 0)
        swap_free  = info.get("SwapFree", 0)
        swap_pct   = (swap_total - swap_free) / swap_total * 100 if swap_total > 0 else 0

        print(f"  ram: {used_pct:.1f}% used")
        print(f"  swap: {swap_pct:.1f}% used ({swap_total // 1024} MB total)")

    except Exception as e:
        print(f"  memory: read failed — {e}")
        return []

    if used_pct >= THRESHOLDS["ram_crit_pct"]:
        alerts.append(Alert("ram", "🧠 RAM Critical",
                            f"RAM {used_pct:.0f}% used — OOM kills likely", "ram", critical=True))
    elif used_pct >= THRESHOLDS["ram_warn_pct"]:
        alerts.append(Alert("ram", "🧠 RAM High",
                            f"RAM {used_pct:.0f}% used", "ram"))

    if swap_total > 0 and swap_pct >= THRESHOLDS["swap_crit_pct"]:
        alerts.append(Alert("swap", "💻 Swap Nearly Full",
                            f"Swap {swap_pct:.0f}% used — OOM kills likely if memory demand rises",
                            "swap", critical=True))
    return alerts


def check_load() -> list[Alert]:
    try:
        load_5m = float(Path("/proc/loadavg").read_text().split()[1])
        print(f"  load (5 min): {load_5m:.2f}")
    except Exception as e:
        print(f"  load: read failed — {e}")
        return []

    if load_5m >= THRESHOLDS["load_warn"]:
        return [Alert("load", "⚡ High CPU Load",
                      f"5-min load average {load_5m:.1f} (Pi 4B has 4 cores)", "load")]
    return []


def check_containers() -> list[Alert]:
    alerts = []
    try:
        result = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}"],
            capture_output=True, text=True, timeout=10,
        )
        running = {line.strip() for line in result.stdout.splitlines() if line.strip()}
    except Exception as e:
        print(f"  containers: docker ps failed — {e}")
        return []

    # Exact names: the old substring test treated "travelnet" as running whenever
    # any container name contained it (e.g. travelnet-nginx).
    for name in EXPECTED_CONTAINERS:
        if name in running:
            print(f"  container {name}: running")
        else:
            print(f"  container {name}: NOT FOUND")
            alerts.append(Alert(
                f"container_{name}", f"🐳 Container Down: {name}",
                f"'{name}' is not running",
                "container", critical=True,
                metadata={"name": name},
            ))
    return alerts


def _docker_inspect(names: list[str]) -> list[dict]:
    try:
        result = subprocess.run(["docker", "inspect", *names],
                                capture_output=True, text=True, timeout=20)
        return json.loads(result.stdout or "[]")
    except Exception as e:
        print(f"  docker inspect failed — {e}")
        return []


def _read_cgroup_events(container_id: str) -> dict[str, int]:
    path = Path(f"/sys/fs/cgroup/system.slice/docker-{container_id}.scope/memory.events")
    try:
        return {k: int(v) for k, v in (ln.split() for ln in path.read_text().splitlines() if ln.strip())}
    except Exception:
        return {}


def evaluate_containers(infos: list[dict], state: dict, read_events=None) -> list[Alert]:
    """Turn `docker inspect` output into alerts: unhealthy, restarted since last run,
    or a cgroup OOM kill since last run. Updates `state` in place."""
    read_events = read_events or _read_cgroup_events
    counts = state.setdefault("restart_counts", {})
    oom = state.setdefault("oom_kill", {})
    seen: set[str] = set()
    alerts: list[Alert] = []

    for info in infos:
        name = info.get("Name", "").lstrip("/")
        st = info.get("State", {})
        health = (st.get("Health") or {}).get("Status")
        restarts = info.get("RestartCount", 0)
        cid = info.get("Id", "")
        key = f"{name}:{cid[:12]}"   # a recreated container gets a fresh baseline
        seen.add(key)
        print(f"  container {name}: status={st.get('Status')} health={health or 'none'} restarts={restarts}")

        if health == "unhealthy":
            alerts.append(Alert(
                f"unhealthy_{name}", f"🩺 Container Unhealthy: {name}",
                f"'{name}' is failing its Docker healthcheck",
                "unhealthy", critical=True, metadata={"name": name},
            ))

        prev = counts.get(key)
        if prev is not None and restarts > prev:
            alerts.append(Alert(
                f"restartloop_{name}", f"🔁 Container Restarted: {name}",
                f"'{name}' restarted {restarts - prev} time(s) since the last check (total {restarts})",
                "restartloop", metadata={"name": name},
            ))
        counts[key] = restarts

        events = read_events(cid)
        if events:
            killed = events.get("oom_kill", 0)
            prev_kill = oom.get(key)
            if prev_kill is not None and killed > prev_kill:
                alerts.append(Alert(
                    f"cgroup_oom_{name}", f"💥 Container OOM Kill: {name}",
                    f"'{name}' hit its memory limit and the kernel killed a process in it "
                    f"({killed - prev_kill} new)",
                    "cgroup_oom", critical=True, metadata={"name": name},
                ))
            oom[key] = killed

    for table in (counts, oom):
        for stale in [k for k in table if k not in seen]:
            del table[stale]
    return alerts


def check_container_health() -> list[Alert]:
    return evaluate_containers(_docker_inspect(EXPECTED_CONTAINERS), STATE)


def check_services() -> list[Alert]:
    alerts = []
    for unit in EXPECTED_SERVICES:
        try:
            result = subprocess.run(["systemctl", "is-active", unit],
                                    capture_output=True, text=True, timeout=10)
            state = result.stdout.strip() or "unknown"
        except Exception as e:
            print(f"  service {unit}: systemctl failed — {e}")
            continue
        print(f"  service {unit}: {state}")
        if state != "active":
            alerts.append(Alert(
                f"service_{unit}", f"⚙️ Service Down: {unit}",
                f"systemd unit '{unit}' is {state}",
                "service", critical=True, metadata={"unit": unit},
            ))
    return alerts


def check_mounts() -> list[Alert]:
    alerts = []
    for mount in REQUIRED_MOUNTS:
        mounted = os.path.ismount(mount)
        print(f"  mount {mount}: {'ok' if mounted else 'NOT MOUNTED'}")
        if not mounted:
            alerts.append(Alert(
                f"mount_{mount}", f"💽 Not Mounted: {mount}",
                f"{mount} is not a mount point — containers and cron jobs that need it will misbehave "
                "(fstab uses nofail, so boot succeeded anyway)",
                "mount", critical=True,
            ))
    return alerts


def check_swap_io() -> list[Alert]:
    try:
        lines = Path("/proc/vmstat").read_text().splitlines()
        pswpin = int(next(ln.split()[1] for ln in lines if ln.startswith("pswpin ")))
    except Exception as e:
        print(f"  swap-in: read failed — {e}")
        return []

    now = time.time()
    rate = swapin_rate(STATE.get("pswpin"), now, pswpin)
    STATE["pswpin"] = {"t": now, "v": pswpin}
    if rate is None:
        print("  swap-in: n/a (first run, too soon, or counter reset)")
        return []
    print(f"  swap-in: {rate:.1f} pages/s")
    if rate >= THRESHOLDS["swapin_pages_per_s"]:
        return [Alert("swapio", "💻 Swap Thrashing",
                      f"{rate:.0f} pages/s ({rate * 4 / 1024:.1f} MB/s) swapped in since the last check "
                      "— active memory pressure, not just cold pages in swap", "swapio")]
    return []


def check_backups() -> list[Alert]:
    try:
        files = list(BACKUP_HOST_DIR.glob("*.db.zst"))
    except Exception as e:
        print(f"  backups: read failed — {e}")
        return []
    if not files:
        print("  backups: none found")
        return [Alert("backup", "🗄️ No DB Backups Found",
                      f"no *.db.zst files in {BACKUP_HOST_DIR}", "backup", critical=True)]
    age_h = (time.time() - max(f.stat().st_mtime for f in files)) / 3600
    print(f"  backups: newest is {age_h:.1f} h old")
    if age_h >= THRESHOLDS["backup_max_age_h"]:
        return [Alert("backup", "🗄️ DB Backup Stale",
                      f"newest backup in {BACKUP_HOST_DIR} is {age_h:.0f} h old "
                      f"(expected daily, limit {THRESHOLDS['backup_max_age_h']} h)", "backup")]
    return []


def check_prefect_db() -> list[Alert]:
    try:
        size_gb = PREFECT_DB_FILE.stat().st_size / 1e9
    except Exception as e:
        print(f"  prefect db: stat failed — {e}")
        return []
    print(f"  prefect db: {size_gb:.2f} GB")
    if size_gb >= THRESHOLDS["prefect_db_warn_gb"]:
        return [Alert("prefectdb", "🗄️ Prefect DB Growing",
                      f"prefect.db is {size_gb:.1f} GB — is the flow-run vacuum still running?",
                      "prefectdb")]
    return []


def check_wal() -> list[Alert]:
    if not WAL_FILE.exists():
        print("  wal: file absent (checkpoint clean)")
        return []
    try:
        size_mb = WAL_FILE.stat().st_size / 1e6
        print(f"  wal: {size_mb:.1f} MB")
    except Exception as e:
        print(f"  wal: stat failed — {e}")
        return []

    if size_mb >= THRESHOLDS["wal_warn_mb"]:
        return [Alert("wal", "🗄️ SQLite WAL File Large",
                      f"WAL is {size_mb:.0f} MB — possible stuck transaction or checkpoint failure",
                      "wal")]
    return []


def check_oom() -> list[Alert]:
    try:
        result = subprocess.run(
            ["journalctl", "-k", "--since", "1 hour ago", "--no-pager", "-q"],
            capture_output=True, text=True, timeout=15,
        )
        output = result.stdout.lower()
    except Exception as e:
        print(f"  oom: journalctl failed — {e}")
        return []

    triggered = any(kw in output for kw in ("out of memory", "oom_kill", "killed process"))
    print(f"  oom: {'DETECTED' if triggered else 'clean'}")

    if triggered:
        return [Alert("oom", "💥 OOM Kill Detected",
                      "Kernel killed a process due to out-of-memory in the last hour",
                      "oom", critical=True)]
    return []


def check_smart() -> list[Alert]:
    if _last_run_within("smart", COOLDOWNS["smart"]):
        print("  smart: skipped (checked within last 24 h)")
        return []
    _mark_last_run("smart")

    alerts = []
    for device, label, dev_type in SMART_DEVICES:
        if not Path(device).exists():
            print(f"  smart {label}: device {device} not found — skipping")
            continue
        try:
            health = subprocess.run(
                ["smartctl", "-H", "-d", dev_type, device],
                capture_output=True, text=True, timeout=20,
            )
            attrs = subprocess.run(
                ["smartctl", "-A", "-d", dev_type, device],
                capture_output=True, text=True, timeout=20,
            )
        except FileNotFoundError:
            print("  smart: smartctl not installed — skipping all SMART checks")
            break
        except Exception as e:
            print(f"  smart {label}: failed — {e}")
            continue

        passed = "PASSED" in health.stdout
        print(f"  smart {label}: {'PASSED' if passed else 'FAILED or unknown'}")

        if "FAILED" in health.stdout:
            alerts.append(Alert(
                f"smart_health_{device}",
                f"🔴 SMART FAILED — {label}",
                f"{device} failed SMART assessment — back up and replace immediately",
                "smart", critical=True,
                metadata={"device": device},
            ))

        for line in attrs.stdout.splitlines():
            if "Reallocated_Sector_Ct" in line or "Current_Pending_Sector" in line:
                parts = line.split()
                raw = int(parts[-1]) if parts and parts[-1].isdigit() else 0
                if raw > 0:
                    print(f"  smart {label}: bad sectors — {parts[1]} = {raw}")
                    alerts.append(Alert(
                        f"smart_sectors_{device}",
                        f"⚠️ SMART Bad Sectors — {label}",
                        f"{device}: {parts[1]} = {raw} (disk degradation detected)",
                        "smart",
                        metadata={"device": device},
                    ))
    return alerts


def check_zombies() -> list[Alert]:
    try:
        result = subprocess.run(["ps", "aux"], capture_output=True, text=True, timeout=10)
        count = sum(1 for line in result.stdout.splitlines() if " Z " in line or " Z+" in line)
        print(f"  zombies: {count}")
    except Exception as e:
        print(f"  zombies: ps failed — {e}")
        return []

    if count >= THRESHOLDS["zombie_warn"]:
        return [Alert("zombie", "🧟 Zombie Processes",
                      f"{count} zombie processes — possible Docker/subprocess leak", "zombie")]
    return []

# ── Mitigations ───────────────────────────────────────────────────────────────

def mitigate_disk(mount: str, label: str) -> str:
    """Run on disk critical (≥ 90%). Prune build cache, old backups, health log."""
    actions = []

    # 1. Docker build cache — biggest win
    result = subprocess.run(
        ["docker", "builder", "prune", "-f"],
        capture_output=True, text=True, timeout=120,
    )
    if result.returncode == 0:
        actions.append("Docker build cache pruned")
    else:
        actions.append(f"Docker prune failed: {result.stderr.strip()[:80]}")

    # 2. Prune old DB backups — keep newest 3 regardless of age
    if BACKUP_HOST_DIR.exists():
        backups = sorted(BACKUP_HOST_DIR.glob("*.db.zst"), key=lambda f: f.stat().st_mtime)
        removed = 0
        for f in backups[:-3]:
            try:
                f.unlink()
                removed += 1
            except Exception:
                pass
        if removed:
            actions.append(f"Pruned {removed} old backup(s) (kept 3)")

    # 3. Truncate health monitor log if over 50 MB
    if HEALTH_LOG.exists() and HEALTH_LOG.stat().st_size > 50 * 1024 * 1024:
        HEALTH_LOG.write_text("")
        actions.append("Truncated health log")

    return ", ".join(actions) if actions else "no actions taken"


def mitigate_container(name: str) -> tuple[bool, str]:
    """Bring a container that is not running back up."""
    result = subprocess.run(
        ["docker", "ps", "-a", "--format", "{{.Names}}"],
        capture_output=True, text=True, timeout=10,
    )
    names = result.stdout.splitlines()
    # Exact match first; the old substring match could pick travelnet-nginx for "travelnet".
    full_name = name if name in names else next(
        (n for n in names if name.lower() in n.lower()), None)
    if not full_name:
        return False, f"No container matching '{name}' found in docker ps -a"

    if full_name in SYSTEMD_MANAGED:
        # The unit removes the container on stop, so `docker start` cannot work.
        return restart_with_budget(full_name, ["systemctl", "restart", SYSTEMD_MANAGED[full_name]])

    restart = subprocess.run(
        ["docker", "start", full_name],
        capture_output=True, text=True, timeout=30,
    )
    if restart.returncode == 0:
        return True, f"Restarted {full_name} successfully"
    return False, f"Failed to restart {full_name}: {restart.stderr.strip()}"


def mitigate_unhealthy(name: str) -> tuple[bool, str]:
    """Restart a container that is failing its healthcheck (rate-limited)."""
    if name in SYSTEMD_MANAGED:
        return restart_with_budget(name, ["systemctl", "restart", SYSTEMD_MANAGED[name]])
    return restart_with_budget(name, ["docker", "restart", name])


def mitigate_service(unit: str) -> tuple[bool, str]:
    if unit not in RESTARTABLE_SERVICES:
        return False, f"no automatic action for {unit}"
    return restart_with_budget(unit, ["systemctl", "restart", f"{unit}.service"])


def mitigate_wal() -> tuple[bool, str, float]:
    """Run WAL checkpoint. Returns (success, message, new_size_mb)."""
    import sqlite3 as _sqlite3
    try:
        conn = _sqlite3.connect(str(DB_HOST_PATH), timeout=10)
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.close()
    except Exception as e:
        return False, f"Checkpoint failed: {e}", -1.0

    new_size_mb = WAL_FILE.stat().st_size / 1e6 if WAL_FILE.exists() else 0.0
    return True, "Checkpoint completed", new_size_mb


def emergency_smart_backup(failing_devices: list[str]) -> str:
    """Back up DB to healthiest available destination, always attempt R2."""
    import sqlite3 as _sqlite3

    ssd_failing = SSD_DEVICE in failing_devices
    hdd_failing = HDD_DEVICE in failing_devices

    if ssd_failing and hdd_failing:
        local_dest   = EMERGENCY_BACKUP_DIRS["sd"]
        dest_label   = "SD card (both drives failing)"
        margin_check = True
    elif hdd_failing:
        local_dest   = EMERGENCY_BACKUP_DIRS["ssd"]
        dest_label   = "SSD (HDD failing)"
        margin_check = False
    else:
        local_dest   = EMERGENCY_BACKUP_DIRS["hdd"]
        dest_label   = "HDD (SSD failing)"
        margin_check = False

    messages = []

    skip_local = False
    if margin_check:
        free = shutil.disk_usage("/").free
        if free < SD_MARGIN_BYTES:
            skip_local = True
            messages.append(f"SD card only {free / 1e9:.1f} GB free — skipping local copy")

    if not skip_local:
        local_dest.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        db_path   = local_dest / f"smart_emergency_{timestamp}.db"
        zst_path  = local_dest / f"smart_emergency_{timestamp}.db.zst"

        try:
            src = _sqlite3.connect(str(DB_HOST_PATH), timeout=10)
            dst = _sqlite3.connect(str(db_path))
            src.backup(dst)
            dst.close()
            src.close()
        except Exception as e:
            messages.append(f"Snapshot failed: {e}")
            db_path = None

        if db_path and db_path.exists():
            try:
                result = subprocess.run(
                    ["zstd", str(db_path), "-o", str(zst_path)],
                    capture_output=True, text=True, timeout=120,
                )
                if result.returncode == 0:
                    db_path.unlink()
                    size_mb = zst_path.stat().st_size / 1e6
                    messages.append(
                        f"Local backup → {dest_label} ({zst_path.name}, {size_mb:.1f} MB)"
                    )
                else:
                    messages.append(f"Compression failed: {result.stderr.strip()}")
            except Exception as e:
                messages.append(f"Compression failed: {e}")

    r2 = subprocess.run(
        [
            "docker", "exec", "-w", "/app", "travelnet",
            "python", "-c",
            "import sys; sys.path.insert(0, '/app'); "
            "from scheduled_tasks.cloudflare_db_backup import cloudflare_backup_db_flow; "
            "cloudflare_backup_db_flow(prefix='smart_emergency')",
        ],
        capture_output=True, text=True, timeout=600,
    )
    messages.append("R2 backup triggered" if r2.returncode == 0
                    else f"R2 backup failed: {r2.stderr.strip()[:120]}")

    return " | ".join(messages)

# ── Main ──────────────────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> None:
    """Run all checks. With --dry-run: report alerts only (no notifications, mitigations,
    cooldown files, state writes, or SMART bookkeeping)."""
    global STATE
    dry_run = "--dry-run" in (sys.argv[1:] if argv is None else argv)
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"\n=== TravelNet health check {now}{' [DRY RUN]' if dry_run else ''} ===")
    if not dry_run:
        COOLDOWN_DIR.mkdir(exist_ok=True)
    STATE = load_state()

    webhook_url = _read_env("WARNING_NOTIFICATION")
    if not webhook_url:
        print(f"WARNING: WARNING_NOTIFICATION not found in {ENV_FILE} — notifications disabled")

    checks = [
        check_mounts,
        check_cpu_temp,
        check_disk,
        check_memory,
        check_swap_io,
        check_load,
        check_containers,
        check_container_health,
        check_services,
        check_wal,
        check_oom,
        check_smart,
        check_zombies,
        check_backups,
        check_prefect_db,
    ]
    if dry_run:
        checks.remove(check_smart)   # it records a last-run marker

    alerts: list[Alert] = []
    for fn in checks:
        try:
            alerts.extend(fn())
        except Exception as e:
            print(f"  {fn.__name__}: CRASHED — {e}")

    fired = 0
    fired_smart_devices: list[str] = []

    for alert in alerts:
        if dry_run:
            print(f"  ALERT (dry run) {'[CRITICAL] ' if alert.critical else ''}{alert.title}: {alert.body}")
            continue
        if _in_cooldown(alert.key, alert.cooldown_key):
            print(f"  suppressed (cooldown): {alert.title}")
            continue

        # ── Mitigations (run before notification so result is included in body) ──

        if alert.cooldown_key == "disk" and alert.critical:
            mount  = alert.metadata.get("mount", "")
            label  = alert.metadata.get("label", "")
            action = mitigate_disk(mount, label)
            print(f"  disk mitigation: {action}")
            alert.body += f" | Auto: {action}"

        elif alert.cooldown_key == "container":
            ok, msg = mitigate_container(alert.metadata.get("name", ""))
            print(f"  container mitigation: {msg}")
            alert.body += f" | Auto: {msg}"

        elif alert.cooldown_key == "unhealthy":
            ok, msg = mitigate_unhealthy(alert.metadata.get("name", ""))
            print(f"  unhealthy mitigation: {msg}")
            alert.body += f" | Auto: {msg}"

        elif alert.cooldown_key == "service":
            ok, msg = mitigate_service(alert.metadata.get("unit", ""))
            print(f"  service mitigation: {msg}")
            alert.body += f" | Auto: {msg}"

        elif alert.cooldown_key == "wal":
            ok, msg, new_mb = mitigate_wal()
            if ok and new_mb < THRESHOLDS["wal_warn_mb"]:
                wal_note = f"checkpoint resolved it ({new_mb:.0f} MB)"
            elif ok:
                wal_note = f"checkpoint ran but WAL still {new_mb:.0f} MB — reader transaction stuck"
                alert.critical = True  # escalate
            else:
                wal_note = msg
            print(f"  wal mitigation: {wal_note}")
            alert.body += f" | Auto: {wal_note}"

        # ── Notify ───────────────────────────────────────────────────────────────

        print(f"  ALERT {'[CRITICAL] ' if alert.critical else ''}{alert.title}: {alert.body}")
        if webhook_url:
            _send(webhook_url, alert.title, alert.body)
        _mark_cooldown(alert.key)
        fired += 1

        if alert.cooldown_key == "smart" and "device" in alert.metadata:
            fired_smart_devices.append(alert.metadata["device"])

    if fired_smart_devices:
        print(f"  smart: emergency backup triggered for {fired_smart_devices}")
        backup_result = emergency_smart_backup(list(set(fired_smart_devices)))
        print(f"  smart: {backup_result}")
        if webhook_url:
            _send(webhook_url, "💾 SMART Emergency Backup", backup_result)

    if not dry_run:
        save_state(STATE)
    print(f"=== {len(alerts)} alert(s), {fired} notification(s) sent{' (dry run: none)' if dry_run else ''} ===\n")


if __name__ == "__main__":
    main()