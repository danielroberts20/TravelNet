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
  • Swap usage             — warn ≥ 30 % (any swap on Pi is a red flag)
  • CPU load average       — warn ≥ 4.0 (Pi 4B has 4 cores)
  • Docker containers      — alert if any expected container is not running
  • SQLite WAL file size   — warn ≥ 100 MB (stuck transaction / checkpoint failure)
  • OOM kill events        — alert if kernel killed a process in the last hour
  • SMART disk health      — FAILED overall + reallocated/pending sectors (once/day)
  • Zombie processes       — warn if ≥ 5 zombies (Docker/subprocess leak)
"""

import json
import shutil
import subprocess
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────

ENV_FILE     = Path("/home/dan/services/TravelNet/server/.env")
COOLDOWN_DIR = Path("/tmp/travelnet_health")
WAL_FILE     = Path("/mnt/ssd/docker/services/travelnet/data/travel.db-wal")

MOUNTS = [
    ("/mnt/ssd",   "SSD"),
    ("/mnt/linux", "HDD"),
    ("/",          "root"),
]

# Substring match against `docker ps` output — case-insensitive
EXPECTED_CONTAINERS = ["server-prefect-worker-1", "travelnet-nginx", "travelnet-dashboard", "travelnet", "prefect-server", "trevor"]

# (device_path, label, smartctl_type)
# Use "sat" for USB enclosures (SAT passthrough). Adjust /dev/sdX to match your setup.
SMART_DEVICES = [
    ("/dev/sda", "SSD", "sat"),    # Samsung 870 EVO 500GB, ASM225CM bridge
    ("/dev/sdb", "HDD", "auto"),   # WD 6TB
]

# ── Thresholds ────────────────────────────────────────────────────────────────

THRESHOLDS = {
    "cpu_temp_warn_c":  70.0,
    "cpu_temp_crit_c":  80.0,
    "disk_warn_pct":    80,
    "disk_crit_pct":    90,
    "ram_warn_pct":     85,
    "ram_crit_pct":     95,
    "swap_warn_pct":    60,
    "load_warn":        4.0,
    "wal_warn_mb":      100,
    "zombie_warn":      5,
}

# Cooldown per alert key in seconds
COOLDOWNS = {
    "cpu_temp":   3_600,   # 1 h
    "disk":      21_600,   # 6 h (per mount)
    "ram":        7_200,   # 2 h
    "swap":       7_200,
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
    key: str          # unique key for cooldown file
    title: str
    body: str
    cooldown_key: str # maps to COOLDOWNS
    critical: bool = False

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
    """Throttle expensive checks (e.g. smartctl) independently of alert cooldown."""
    p = COOLDOWN_DIR / f"lastrun_{key}"
    if not p.exists():
        return False
    return (datetime.now(timezone.utc).timestamp() - p.stat().st_mtime) < seconds


def _mark_last_run(key: str) -> None:
    COOLDOWN_DIR.mkdir(exist_ok=True)
    (COOLDOWN_DIR / f"lastrun_{key}").touch()

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
        pct      = usage.used / usage.total * 100
        free_gb  = usage.free / 1e9
        print(f"  disk {label} ({mount}): {pct:.1f}% used, {free_gb:.1f} GB free")
        key = f"disk_{mount}"
        if pct >= THRESHOLDS["disk_crit_pct"]:
            alerts.append(Alert(key, f"💾 Disk Critical — {label}",
                                f"{mount} is {pct:.0f}% full ({free_gb:.1f} GB free)", "disk", critical=True))
        elif pct >= THRESHOLDS["disk_warn_pct"]:
            alerts.append(Alert(key, f"💾 Disk Warning — {label}",
                                f"{mount} is {pct:.0f}% full ({free_gb:.1f} GB free)", "disk"))
    return alerts


def check_memory() -> list[Alert]:
    alerts = []
    try:
        info = {}
        for line in Path("/proc/meminfo").read_text().splitlines():
            k, _, v = line.partition(":")
            info[k.strip()] = int(v.strip().split()[0])  # kB

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

    if swap_total > 0 and swap_pct >= THRESHOLDS["swap_warn_pct"]:
        alerts.append(Alert("swap", "💻 Swap In Use",
                            f"Swap {swap_pct:.0f}% used — Pi is under memory pressure", "swap"))
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
            ["docker", "ps", "--format", "{{.Names}}\t{{.Status}}"],
            capture_output=True, text=True, timeout=10,
        )
        running = result.stdout.lower()
    except Exception as e:
        print(f"  containers: docker ps failed — {e}")
        return []

    for name in EXPECTED_CONTAINERS:
        if name.lower() in running:
            print(f"  container {name}: OK")
        else:
            print(f"  container {name}: NOT FOUND")
            alerts.append(Alert(f"container_{name}", f"🐳 Container Down: {name}",
                                f"'{name}' is not running", "container", critical=True))
    return alerts


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
                      f"WAL is {size_mb:.0f} MB — possible stuck transaction or checkpoint failure", "wal")]
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
            alerts.append(Alert(f"smart_health_{device}", f"🔴 SMART FAILED — {label}",
                                f"{device} failed SMART assessment — back up and replace immediately",
                                "smart", critical=True))

        for line in attrs.stdout.splitlines():
            if "Reallocated_Sector_Ct" in line or "Current_Pending_Sector" in line:
                parts = line.split()
                raw = int(parts[-1]) if parts and parts[-1].isdigit() else 0
                if raw > 0:
                    print(f"  smart {label}: bad sectors — {parts[1]} = {raw}")
                    alerts.append(Alert(f"smart_sectors_{device}", f"⚠️ SMART Bad Sectors — {label}",
                                        f"{device}: {parts[1]} = {raw} (disk degradation detected)",
                                        "smart"))
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

# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"\n=== TravelNet health check {now} ===")
    COOLDOWN_DIR.mkdir(exist_ok=True)

    webhook_url = _read_env("WARNING_NOTIFICATION")
    if not webhook_url:
        print(f"WARNING: WARNING_NOTIFICATION not found in {ENV_FILE} — notifications disabled")

    checks = [
        check_cpu_temp,
        check_disk,
        check_memory,
        check_load,
        check_containers,
        check_wal,
        check_oom,
        check_smart,
        check_zombies,
    ]

    alerts: list[Alert] = []
    for fn in checks:
        try:
            alerts.extend(fn())
        except Exception as e:
            print(f"  {fn.__name__}: CRASHED — {e}")

    fired = 0
    for alert in alerts:
        if _in_cooldown(alert.key, alert.cooldown_key):
            print(f"  suppressed (cooldown): {alert.title}")
            continue
        print(f"  ALERT {'[CRITICAL] ' if alert.critical else ''}{alert.title}: {alert.body}")
        if webhook_url:
            _send(webhook_url, alert.title, alert.body)
        _mark_cooldown(alert.key)
        fired += 1

    print(f"=== {len(alerts)} alert(s), {fired} notification(s) sent ===\n")


if __name__ == "__main__":
    main()