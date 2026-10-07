#!/usr/bin/env python3
"""
reap_idle_dev_sessions.py
Stop Claude Code dev sessions on this Pi that have been idle for hours.

Why: this Pi is the production host for TravelNet, and idle dev sessions keep
holding RAM (and, once the dev slice is over its limit, swap). Sessions started
from the desktop app are not always closed when the app goes away, so they
linger for many hours. Their transcripts stay on disk, so a stopped session can
be resumed.

A session is a process whose program path (argv[0]) contains --match
(default /.claude/remote/ccd-cli/). It counts as ACTIVE when either:
  * a transcript in its project directory (~/.claude/projects/<cwd, non-alphanumerics
    replaced by "-">/*.jsonl) was written within --idle-hours, or
  * its process tree used CPU faster than --cpu-rate between two runs (a long test
    run or build keeps a session alive even if nothing is written to the transcript).
Otherwise the idle clock runs. A session idle for --idle-hours (default 3) is
stopped (SIGTERM to the tree, SIGKILL after --grace seconds) and ONE Pushcut
notification lists what was stopped.

Safety:
  * never touches the process chain that started this script
  * only argv[0] is matched (a shell command that merely mentions the path is not a session)
  * a session must be seen on two runs before it can be stopped (the CPU rule needs a baseline)
  * PID reuse is guarded by comparing process start times
  * at most --max-reaped sessions per run
  * --dry-run reports (once per session) what it WOULD stop and kills nothing

Meant to run every 30 minutes from a systemd user timer; see
docs/drafts/dev-session-reaper.{service,timer}.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import sys
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path

CLK_TCK = os.sysconf("SC_CLK_TCK")
DEFAULT_MATCH = "/.claude/remote/ccd-cli/"
DEFAULT_ENV_FILE = "/home/dan/services/TravelNet/server/.env"
DEFAULT_NOTIFY_VAR = "CUSTOM_NOTIFICATION_NOT_TIME_SENSITIVE"
DEFAULT_STATE_FILE = "~/.local/state/dev-session-reaper/state.json"
DEFAULT_PROJECTS_DIR = "~/.claude/projects"


def log(msg: str) -> None:
    print(f"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} {msg}", flush=True)


# ── /proc access ──────────────────────────────────────────────────────────────

@dataclass
class Proc:
    pid: int
    ppid: int
    cpu_ticks: int      # utime + stime + cutime + cstime
    start_ticks: int    # process start time since boot, in clock ticks


def read_stat(pid: int, proc_root: str = "/proc") -> Proc | None:
    try:
        text = Path(proc_root, str(pid), "stat").read_text()
    except OSError:
        return None
    # "pid (comm) state ppid ..." where comm may contain spaces and parentheses.
    _, _, rest = text.rpartition(")")
    f = rest.split()
    try:
        return Proc(
            pid=pid,
            ppid=int(f[1]),
            cpu_ticks=int(f[11]) + int(f[12]) + int(f[13]) + int(f[14]),
            start_ticks=int(f[19]),
        )
    except (IndexError, ValueError):
        return None


def snapshot(proc_root: str = "/proc") -> dict[int, Proc]:
    snap = {}
    for entry in os.listdir(proc_root):
        if entry.isdigit():
            p = read_stat(int(entry), proc_root)
            if p:
                snap[p.pid] = p
    return snap


def argv(pid: int, proc_root: str = "/proc") -> list[str]:
    try:
        raw = Path(proc_root, str(pid), "cmdline").read_bytes()
    except OSError:
        return []
    return [a.decode(errors="replace") for a in raw.split(b"\0") if a]


def age_seconds(proc: Proc, proc_root: str = "/proc") -> float | None:
    """How long the process has been running, from /proc/uptime and its start tick."""
    try:
        uptime = float(Path(proc_root, "uptime").read_text().split()[0])
    except (OSError, ValueError, IndexError):
        return None
    return max(0.0, uptime - proc.start_ticks / CLK_TCK)


def cwd_of(pid: int, proc_root: str = "/proc") -> str | None:
    try:
        return os.readlink(Path(proc_root, str(pid), "cwd"))
    except OSError:
        return None


def memory_kb(pid: int, proc_root: str = "/proc") -> tuple[int, int]:
    """(resident, swapped) in kB, from /proc/<pid>/status."""
    rss = swap = 0
    try:
        for line in Path(proc_root, str(pid), "status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                rss = int(line.split()[1])
            elif line.startswith("VmSwap:"):
                swap = int(line.split()[1])
    except (OSError, ValueError, IndexError):
        pass
    return rss, swap


def descendants(snap: dict[int, Proc], root: int) -> list[int]:
    kids: dict[int, list[int]] = {}
    for p in snap.values():
        kids.setdefault(p.ppid, []).append(p.pid)
    out, stack = [], [root]
    while stack:
        cur = stack.pop()
        for k in kids.get(cur, []):
            out.append(k)
            stack.append(k)
    return out


def ancestors(snap: dict[int, Proc], pid: int) -> set[int]:
    out, cur = set(), snap[pid].ppid if pid in snap else 0
    while cur > 1 and cur not in out:
        out.add(cur)
        cur = snap[cur].ppid if cur in snap else 0
    return out


def find_sessions(snap: dict[int, Proc], match: str, proc_root: str = "/proc") -> list[int]:
    """Top-level session processes: argv[0] contains `match`, owned by this user."""
    uid = os.getuid()
    found = []
    for pid in snap:
        a = argv(pid, proc_root)
        if not a or match not in a[0]:
            continue
        try:
            if Path(proc_root, str(pid)).stat().st_uid != uid:
                continue
        except OSError:
            continue
        found.append(pid)
    nested = {p for p in found if ancestors(snap, p) & set(found)}
    return sorted(set(found) - nested)


def transcript_mtime(cwd: str | None, projects_dir: Path) -> float | None:
    """Newest transcript write in the session's project directory, if any."""
    if not cwd:
        return None
    d = projects_dir / re.sub(r"[^A-Za-z0-9]", "-", cwd)
    try:
        return max((f.stat().st_mtime for f in d.glob("*.jsonl")), default=None)
    except OSError:
        return None


# ── decision ──────────────────────────────────────────────────────────────────

def tree_cpu_ticks(snap: dict[int, Proc], root: int) -> int:
    return snap[root].cpu_ticks + sum(snap[p].cpu_ticks for p in descendants(snap, root))


def update_entry(entry: dict | None, total_ticks: int, transcript: float | None, now: float,
                 cpu_rate: float) -> dict:
    """Return the new state entry for a session after observing it at `now`."""
    if entry is None:
        # First sight: no CPU baseline yet. The transcript tells us how long it has been quiet.
        return {"first_seen": now, "checked_at": now, "last_total": total_ticks,
                "last_active": transcript if transcript else now, "observations": 1}

    last_active = max(entry["last_active"], transcript or 0.0)
    elapsed = now - entry["checked_at"]
    if elapsed > 0 and (total_ticks - entry["last_total"]) / CLK_TCK / elapsed > cpu_rate:
        last_active = now
    new = dict(entry)
    new.update(checked_at=now, last_total=total_ticks, last_active=last_active,
               observations=entry.get("observations", 1) + 1)
    return new


def is_idle(entry: dict, now: float, idle_s: float) -> bool:
    return entry["observations"] >= 2 and now - entry["last_active"] >= idle_s


# ── acting ────────────────────────────────────────────────────────────────────

def same_process(pid: int, start_ticks: int, proc_root: str = "/proc") -> bool:
    p = read_stat(pid, proc_root)
    return p is not None and p.start_ticks == start_ticks


def stop_tree(pids: list[int], starts: dict[int, int], grace_s: float, *, kill=os.kill,
              sleep=time.sleep, proc_root: str = "/proc") -> list[int]:
    """SIGTERM the whole tree, wait, SIGKILL survivors. Returns the pids that needed SIGKILL."""
    for p in pids:
        if same_process(p, starts[p], proc_root):
            try:
                kill(p, signal.SIGTERM)
            except ProcessLookupError:
                pass
    deadline = time.time() + grace_s
    while time.time() < deadline and any(same_process(p, starts[p], proc_root) for p in pids):
        sleep(min(0.5, max(0.0, deadline - time.time())))
    killed = []
    for p in pids:
        if same_process(p, starts[p], proc_root):
            try:
                kill(p, signal.SIGKILL)
                killed.append(p)
            except ProcessLookupError:
                pass
    return killed


def fmt_duration(seconds: float) -> str:
    h, m = divmod(int(seconds) // 60, 60)
    return f"{h}h{m:02d}m" if h else f"{m}m"


def read_env_var(path: str, name: str) -> str | None:
    try:
        for line in Path(path).read_text().splitlines():
            line = line.strip()
            if line.startswith(f"{name}=") and not line.startswith("#"):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    except OSError:
        pass
    return None


def send_pushcut(url: str, title: str, text: str) -> None:
    req = urllib.request.Request(
        url, data=json.dumps({"title": title, "text": text}).encode(),
        headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=10):
        pass


def load_state(path: Path) -> dict:
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=1))
    tmp.replace(path)


# ── main ──────────────────────────────────────────────────────────────────────

def main(argv_: list[str] | None = None, *, now: float | None = None, proc_root: str = "/proc",
         kill=os.kill, notify=None, sleep=time.sleep) -> list[dict]:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--dry-run", action="store_true", help="report what would be stopped; stop nothing")
    ap.add_argument("--idle-hours", type=float, default=3.0)
    ap.add_argument("--cpu-rate", type=float, default=0.05,
                    help="CPU seconds per second (over the whole tree) above which a session counts as active")
    ap.add_argument("--grace", type=float, default=30.0, help="seconds between SIGTERM and SIGKILL")
    ap.add_argument("--max-reaped", type=int, default=5, help="refuse to stop more than this many in one run")
    ap.add_argument("--match", default=DEFAULT_MATCH)
    ap.add_argument("--state-file", default=DEFAULT_STATE_FILE)
    ap.add_argument("--projects-dir", default=DEFAULT_PROJECTS_DIR)
    ap.add_argument("--env-file", default=DEFAULT_ENV_FILE)
    ap.add_argument("--notify-var", default=DEFAULT_NOTIFY_VAR)
    args = ap.parse_args(argv_)

    now = time.time() if now is None else now
    idle_s = args.idle_hours * 3600
    state_path = Path(args.state_file).expanduser()
    projects_dir = Path(args.projects_dir).expanduser()
    mode = "DRY RUN " if args.dry_run else ""

    snap = snapshot(proc_root)
    sessions = find_sessions(snap, args.match, proc_root)
    protected = ancestors(snap, os.getpid()) | {os.getpid()}
    old_state = load_state(state_path)
    new_state: dict[str, dict] = {}
    candidates: list[tuple[int, str, dict]] = []

    for pid in sessions:
        key = f"{pid}:{snap[pid].start_ticks}"
        cwd = cwd_of(pid, proc_root)
        entry = update_entry(old_state.get(key), tree_cpu_ticks(snap, pid),
                             transcript_mtime(cwd, projects_dir), now, args.cpu_rate)
        new_state[key] = entry
        idle_for = now - entry["last_active"]
        log(f"{mode}session pid={pid} cwd={cwd} idle={fmt_duration(idle_for)} seen={entry['observations']}x")
        if is_idle(entry, now, idle_s):
            if pid in protected or (set(descendants(snap, pid)) | {pid}) & protected:
                log(f"  idle but protected (it is part of the process chain running this script): pid={pid}")
                continue
            candidates.append((pid, key, entry))

    if len(candidates) > args.max_reaped:
        log(f"{len(candidates)} idle sessions exceeds --max-reaped={args.max_reaped}; refusing to stop any")
        save_state(state_path, new_state)
        return []

    results = []
    for pid, key, entry in candidates:
        if args.dry_run and new_state[key].get("notified") == "dry-run":
            continue    # already reported this one
        tree = [pid] + descendants(snap, pid)
        mem = [memory_kb(p, proc_root) for p in tree]
        result = {
            "pid": pid, "cwd": cwd_of(pid, proc_root) or "?",
            "idle_s": now - entry["last_active"],
            "age_s": age_seconds(snap[pid], proc_root),
            "rss_mb": sum(m[0] for m in mem) // 1024, "swap_mb": sum(m[1] for m in mem) // 1024,
            "procs": len(tree), "forced": [],
        }
        if args.dry_run:
            new_state[key]["notified"] = "dry-run"
            log(f"DRY RUN would stop pid={pid} ({result['procs']} procs, ~{result['rss_mb']} MB RAM)")
        else:
            starts = {p: snap[p].start_ticks for p in tree}
            result["forced"] = stop_tree(tree, starts, args.grace, kill=kill, sleep=sleep, proc_root=proc_root)
            log(f"stopped pid={pid} ({result['procs']} procs, ~{result['rss_mb']} MB RAM, "
                f"{len(result['forced'])} needed SIGKILL)")
            new_state.pop(key, None)
        results.append(result)

    if results:
        lines = [f"• {r['cwd']}: idle {fmt_duration(r['idle_s'])}"
                 + (f", up {fmt_duration(r['age_s'])}" if r["age_s"] is not None else "")
                 + f", ~{r['rss_mb']} MB RAM" + (f" + {r['swap_mb']} MB swap" if r["swap_mb"] else "")
                 for r in results]
        if args.dry_run:
            title, text = "🧹 Idle dev session (dry run)", "Would stop:\n" + "\n".join(lines)
        else:
            title, text = "🧹 Dev session stopped", "Idle ≥ %s, resumable from its transcript:\n%s" % (
                fmt_duration(idle_s), "\n".join(lines))
        try:
            if notify is not None:
                notify(title, text)
            else:
                url = read_env_var(args.env_file, args.notify_var)
                if url:
                    send_pushcut(url, title, text)
                else:
                    log(f"no {args.notify_var} in {args.env_file}; notification skipped")
        except Exception as e:      # a failed notification must never undo or hide the work
            log(f"notification failed: {e}")

    save_state(state_path, new_state)
    return results


if __name__ == "__main__":
    main()
