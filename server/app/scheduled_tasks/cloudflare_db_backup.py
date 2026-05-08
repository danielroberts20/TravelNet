# scheduled_tasks/backup_db_to_cloudflare.py

import os
import shutil
import signal
import sqlite3
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from prefect import flow, get_run_logger

from config.general import DB_FILE
from config.settings import settings
from notifications import notify_on_completion, record_flow_result


def _run(cmd: list[str], timeout: int = 120) -> subprocess.CompletedProcess:
    """Run a subprocess, raise on non-zero exit with stderr included."""
    logger = get_run_logger()
    logger.debug("Running: %s", " ".join(cmd))
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Command failed ({result.returncode}): {' '.join(cmd)}\n"
            f"stderr: {result.stderr.strip()}"
        )
    return result


def _snapshot_db(dest: Path) -> None:
    """Use SQLite online backup API — safe while DB is live and in WAL mode."""
    src_conn = sqlite3.connect(str(DB_FILE))
    dst_conn = sqlite3.connect(str(dest))
    src_conn.backup(dst_conn)
    dst_conn.close()
    src_conn.close()


def _verify_backup(path: Path) -> int:
    """Integrity check + row count. Returns total row count."""
    conn = sqlite3.connect(str(path))
    row = conn.execute("PRAGMA integrity_check;").fetchone()
    if row[0] != "ok":
        raise RuntimeError(f"Integrity check failed: {row[0]}")
    tables = [
        r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
        ).fetchall()
    ]
    total = sum(
        conn.execute(f"SELECT COUNT(*) FROM [{t}];").fetchone()[0]
        for t in tables
    )
    conn.close()
    return total


def _encrypt(src: Path, dest: Path, key_path: str) -> None:
    """
    Encrypt with age. 

    age encryption to a secret key identity requires extracting the public key
    first with `age-keygen -y`, then encrypting with `-r <pubkey>`.
    This avoids the silent failure caused by using -i (identity flag) 
    on the encrypt side.
    """
    # Derive public key from the identity file
    result = subprocess.run(
        ["age-keygen", "-y", key_path],
        capture_output=True,
        text=True,
        timeout=10,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Failed to derive public key from identity: {result.stderr.strip()}"
        )
    public_key = result.stdout.strip()

    _run([
        "age", "--encrypt",
        "--recipient", public_key,
        "--output", str(dest),
        str(src),
    ])


def _upload_to_r2(path: Path, dir: str) -> None:
    _run([
        "rclone", "copy",
        str(path),
        f"{settings.rclone_remote}:{settings.rclone_bucket}/{dir}/",
    ], timeout=180)


def _verify_upload(filename: str) -> int:
    """Return size in bytes of the uploaded file as reported by rclone."""
    result = _run([
        "rclone", "ls",
        f"{settings.rclone_remote}:{settings.rclone_bucket}/{filename}",
    ])
    # rclone ls output: "  <size> <filename>"
    parts = result.stdout.strip().split()
    if not parts:
        raise RuntimeError(f"File not found in R2 after upload: {filename}")
    return int(parts[0])


@flow(
    name="Cloudflare DB Backup",
    on_failure=[notify_on_completion],
)
def cloudflare_backup_db_flow(
    prefix: str | None = None,
    suffix: str | None = None,
    directory: str = "travelnet-backup"
):
    logger = get_run_logger()

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    parts = [p for p in [prefix, timestamp, suffix] if p]
    stem = "_".join(parts)
    db_filename = f"{stem}.db"
    age_filename = f"{stem}.db.age"

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        snapshot_path = tmp_path / db_filename
        encrypted_path = tmp_path / age_filename

        # Step 1 — snapshot
        logger.info("Taking DB snapshot → %s", snapshot_path.name)
        _snapshot_db(snapshot_path)
        snapshot_size_mb = snapshot_path.stat().st_size / 1_048_576
        logger.info("Snapshot size: %.1f MB", snapshot_size_mb)

        # Step 2 — verify snapshot before encrypting
        logger.info("Verifying snapshot integrity…")
        row_count = _verify_backup(snapshot_path)
        logger.info("Integrity OK — %d total rows", row_count)

        # Step 3 — encrypt
        logger.info("Encrypting → %s", age_filename)
        _encrypt(snapshot_path, encrypted_path, settings.age_key_path)
        encrypted_size_mb = encrypted_path.stat().st_size / 1_048_576
        if encrypted_size_mb < 0.01:
            raise RuntimeError(
                f"Encrypted file suspiciously small ({encrypted_size_mb:.3f} MB) "
                f"— encryption likely failed silently"
            )
        logger.info("Encrypted size: %.1f MB", encrypted_size_mb)

        # Step 4 — upload
        logger.info("Uploading to R2…")
        _upload_to_r2(encrypted_path, directory)

        # Step 5 — verify upload
        logger.info("Verifying upload…")
        uploaded_bytes = _verify_upload(f"{directory}/{age_filename}")
        local_bytes = encrypted_path.stat().st_size
        if uploaded_bytes != local_bytes:
            raise RuntimeError(
                f"Upload size mismatch: local={local_bytes}B, "
                f"R2={uploaded_bytes}B"
            )
        logger.info("Upload verified: %d bytes", uploaded_bytes)

    result = {
        "filename": age_filename,
        "snapshot_size_mb": round(snapshot_size_mb, 1),
        "encrypted_size_mb": round(encrypted_size_mb, 1),
        "row_count": row_count,
        "prefix": prefix,
        "suffix": suffix,
    }
    record_flow_result(result)
    return result