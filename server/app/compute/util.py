import paramiko
import os
import threading
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
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        settings.compute_host,
        port=settings.compute_port,
        username=settings.compute_username,
        password=settings.compute_password,
        timeout=5
    )
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
                "docker ps -q | xargs -r docker stop; /mnt/c/Windows/System32/shutdown.exe /s /t 0",
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