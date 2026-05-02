import os
import signal
import shutil
import sqlite3
import subprocess
import tempfile
from pathlib import Path
from typing import Generator

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse

from app.auth import require_upload_token
from app.config.settings import settings
from app.config.general import DB_FILE

router = APIRouter()


def _sse(message: str, level: str = "info") -> str:
    return f"data: {level}|{message}\n\n"


@router.get("/list", dependencies=[Depends(require_upload_token)])
def list_r2_backups():
    """List .db.age backups available in Cloudflare R2."""
    try:
        result = subprocess.run(
            ["rclone", "ls", f"{settings.rclone_remote}:{settings.rclone_bucket}"],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode != 0:
            return {"error": result.stderr.strip(), "backups": []}

        backups = []
        for line in result.stdout.splitlines():
            parts = line.strip().split(None, 1)
            if len(parts) == 2 and parts[1].endswith(".db.age"):
                size_bytes, filename = parts
                backups.append({
                    "filename": filename.strip(),
                    "size_bytes": int(size_bytes),
                })

        return {
            "backups": sorted(backups, key=lambda x: x["filename"], reverse=True)
        }

    except FileNotFoundError:
        return {"error": "rclone not found in container", "backups": []}
    except subprocess.TimeoutExpired:
        return {"error": "rclone timed out listing R2", "backups": []}


def _restore_stream(filename: str, live: bool) -> Generator[str, None, None]:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        encrypted = tmp_path / filename
        decrypted = tmp_path / filename.removesuffix(".age")

        # Step 1: Download
        yield _sse(f"── Step 1: Downloading {filename} from R2…")
        r = subprocess.run(
            ["rclone", "copy",
             f"{settings.rclone_remote}:{settings.rclone_bucket}/{filename}",
             str(tmp_path)],
            capture_output=True, text=True, timeout=120
        )
        if r.returncode != 0:
            yield _sse(f"✗ Download failed: {r.stderr.strip()}", "error")
            return
        yield _sse("    ✓ Downloaded")

        # Step 2: Decrypt
        yield _sse("── Step 2: Decrypting…")
        r = subprocess.run(
            ["age", "--decrypt", "-i", settings.age_key_path,
             "-o", str(decrypted), str(encrypted)],
            capture_output=True, text=True, timeout=60
        )
        if r.returncode != 0:
            yield _sse(f"✗ Decryption failed: {r.stderr.strip()}", "error")
            return
        yield _sse("    ✓ Decrypted")

        # Step 3: Integrity check
        yield _sse("── Step 3: Integrity check…")
        try:
            conn = sqlite3.connect(str(decrypted))
            row = conn.execute("PRAGMA integrity_check;").fetchone()
            conn.close()
            if row[0] != "ok":
                yield _sse(f"✗ Integrity check FAILED: {row[0]}", "error")
                return
            yield _sse("    ✓ Integrity check passed")
        except Exception as e:
            yield _sse(f"✗ Integrity check error: {e}", "error")
            return

        # Step 4: Row counts
        yield _sse("── Step 4: Row counts…")
        try:
            conn = sqlite3.connect(str(decrypted))
            tables = [r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
            ).fetchall()]
            for table in tables:
                count = conn.execute(
                    f"SELECT COUNT(*) FROM [{table}];"
                ).fetchone()[0]
                yield _sse(f"    {table:<35} {count:>8,} rows")
            conn.close()
        except Exception as e:
            yield _sse(f"✗ Row count error: {e}", "error")
            return

        yield _sse("── Backup looks healthy.")
        yield _sse("")

        if not live:
            yield _sse(
                "ℹ  Dry run complete. No changes made to the live database.",
                "success"
            )
            return

        # Step 5: Replace live database
        yield _sse("── Step 5: Replacing live database…")
        try:
            # Checkpoint WAL before replacement so the copy is clean
            live_conn = sqlite3.connect(str(DB_FILE))
            live_conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            live_conn.close()
            shutil.copy2(str(decrypted), str(DB_FILE))
            yield _sse("    ✓ Database replaced")
        except Exception as e:
            yield _sse(f"✗ Replace failed: {e}", "error")
            return

        yield _sse("── Step 6: Restarting ingest service…")
        yield _sse(
            "    Connection will drop. Wait ~1 minute then reload the Dashboard.",
            "success"
        )

        # SIGTERM triggers uvicorn graceful shutdown;
        # Docker restart policy brings the container back automatically.
        os.kill(os.getpid(), signal.SIGTERM)


@router.get("/stream", dependencies=[Depends(require_upload_token)])
def stream_restore(
    filename: str = Query(...),
    live: bool = Query(False),
):
    """SSE stream for backup restore. Set live=true to replace the live database."""
    return StreamingResponse(
        _restore_stream(filename, live),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )