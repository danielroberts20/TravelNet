"""
compute/ssh.py
~~~~~~~~~~~~~~
SSH client helpers and Wake-on-LAN utilities for the remote compute (PC) node.

Provides:
  - ssh_run()       — execute a command on the remote host in a daemon thread
  - wake_pc()       — send a WoL magic packet and start SSH polling
  - shutdown_pc()   — stop Docker containers then issue a Windows shutdown
  - is_pc_active()  — check the current SSH reachability state
  - get_last_wol()  — timestamp of the last WoL command sent
"""

import paramiko
import os
import threading
from pathlib import Path
from datetime import datetime, timezone
import time
from config.settings import settings
from notifications import send_notification

import logging
logging.getLogger("paramiko").setLevel(41)

# Global state
pc_active = False
_poll_thread: threading.Thread | None = None


def get_ssh_client() -> paramiko.SSHClient:
    """Connect to the compute host.

    Authentication: the private key at COMPUTE_SSH_KEY_PATH is tried first, then the password
    (paramiko's order), so the key can be rolled out before the password is removed.

    Host key: with COMPUTE_KNOWN_HOSTS_PATH set, only the pinned key is accepted (a changed or
    unknown key raises). Without it the previous trust-on-first-use behaviour is kept.
    """
    client = paramiko.SSHClient()

    known_hosts = settings.compute_known_hosts_path
    if known_hosts and Path(known_hosts).is_file():
        client.load_host_keys(known_hosts)
        client.set_missing_host_key_policy(paramiko.RejectPolicy())
    else:
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    kwargs = dict(
        port=settings.compute_port,
        username=settings.compute_username,
        timeout=5,
        allow_agent=False,
        look_for_keys=False,
    )
    key_path = settings.compute_ssh_key_path
    if key_path and Path(key_path).is_file():
        kwargs["key_filename"] = key_path
    if settings.compute_password:
        kwargs["password"] = settings.compute_password
    if "key_filename" not in kwargs and "password" not in kwargs:
        raise RuntimeError("No SSH credentials for the compute host (set COMPUTE_SSH_KEY_PATH or COMPUTE_PASSWORD)")

    client.connect(settings.compute_host, **kwargs)
    return client


def ssh_run(command: str, callback=None) -> threading.Thread:
    def _run():
        client = get_ssh_client()
        try:
            _, stdout, stderr = client.exec_command(command, get_pty=True)
            out = stdout.read().decode()
            err = stderr.read().decode()
            if callback:
                callback(out, err)
        finally:
            client.close()

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    return thread


def _poll_ssh(interval: int = 10):
    global pc_active
    previous_state = None

    while True:
        try:
            client = get_ssh_client()
            client.close()
            current_state = True
        except Exception:
            current_state = False

        if current_state != previous_state:
            if current_state:
                send_notification(title="💻  PC Online", body="✅ Compute service is now available")
            else:
                if previous_state is not None:  # avoid notifying on first poll if already offline
                    send_notification(title="💻  PC Offline", body="❌ Compute service is no longer available.")
            previous_state = current_state

        pc_active = current_state
        time.sleep(interval)


def wake_pc():
    global _poll_thread
    # Send magic packet via WoL service on Pi
    import requests
    requests.post(
        f"http://{settings.wol_host}:9000/wake",
        params={"api_key": settings.wol_api_key},
        timeout=5
    )

    with open("/tmp/last_wol_sent", "w") as f:
        f.write(datetime.now(timezone.utc).isoformat())

    # Start polling if not already running
    if _poll_thread is None or not _poll_thread.is_alive():
        _poll_thread = threading.Thread(target=_poll_ssh, daemon=True)
        _poll_thread.start()


def is_pc_active() -> bool:
    return pc_active


def shutdown_pc(callback=None) -> threading.Thread:
    def _run():
        client = get_ssh_client()
        try:
            client.exec_command(
                "docker ps -q | xargs -r docker stop; /mnt/c/Windows/System32/shutdown.exe /s /f /t 0",
                get_pty=False
            )
            time.sleep(2)  # give it a moment to fire before closing
        finally:
            client.close()
        global pc_active
        pc_active = False
        if callback:
            callback("", "")

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    return thread


def get_last_wol():
    try:
        with open("/tmp/last_wol_sent", "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        return "1970-01-01T00:00:00+00:00"
