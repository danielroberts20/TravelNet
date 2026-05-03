import json
from dataclasses import dataclass, field
from typing import Optional

from database.base import BaseTable
from database.connection import get_conn


@dataclass
class PhotoMetadataRecord:
    file_path: str
    taken_at: str
    is_screenshot: int
    is_screen_recording: int
    is_favourite: int
    filename: Optional[str] = None
    file_extension: Optional[str] = None
    file_size_bytes: Optional[int] = None
    local_date: Optional[str] = None
    date_created: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    gps_source: Optional[str] = None
    track_join_delta_s: Optional[int] = None
    place_id: Optional[int] = None
    media_type: Optional[str] = None
    photo_type: Optional[str] = None
    width: Optional[int] = None
    height: Optional[int] = None
    duration_s: Optional[float] = None
    source_app: Optional[str] = None
    camera_make: Optional[str] = None
    camera_model: Optional[str] = None
    album: Optional[str] = None
    raw_exif: Optional[str] = None
    sentiment_score: Optional[float] = None
    dominant_emotion: Optional[str] = None


class PhotoMetadataTable(BaseTable[PhotoMetadataRecord]):

    def init(self) -> None:
        with get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS photo_metadata (
                    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_path           TEXT NOT NULL,
                    filename            TEXT,
                    file_extension      TEXT,
                    file_size_bytes     INTEGER,
                    taken_at            TEXT NOT NULL,
                    local_date          TEXT,
                    date_created        TEXT,
                    latitude            REAL,
                    longitude           REAL,
                    gps_source          TEXT,
                    track_join_delta_s  INTEGER,
                    place_id            INTEGER REFERENCES places(id),
                    media_type          TEXT,
                    photo_type          TEXT,
                    width               INTEGER,
                    height              INTEGER,
                    duration_s          REAL,
                    is_screenshot       INTEGER NOT NULL DEFAULT 0,
                    is_screen_recording INTEGER NOT NULL DEFAULT 0,
                    is_favourite        INTEGER NOT NULL DEFAULT 0,
                    source_app          TEXT,
                    camera_make         TEXT,
                    camera_model        TEXT,
                    album               TEXT,
                    raw_exif            TEXT,
                    sentiment_score     REAL,
                    dominant_emotion    TEXT,
                    created_at          TEXT NOT NULL
                        DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
                    UNIQUE(file_path)
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_photos_taken_at
                ON photo_metadata(taken_at)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_photos_local_date
                ON photo_metadata(local_date)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_photos_place
                ON photo_metadata(place_id)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_photos_source_app
                ON photo_metadata(source_app)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_photos_backfill
                ON photo_metadata(taken_at)
                WHERE place_id IS NULL AND latitude IS NOT NULL
            """)

    def insert(self, record: PhotoMetadataRecord) -> bool:
        """Insert a photo metadata row.

        Uses INSERT OR IGNORE on UNIQUE(file_path) — re-uploading the same
        batch is safe, duplicates are silently skipped.
        Returns True if inserted, False if ignored.
        """
        # Serialise raw_exif dict to JSON string if caller passed a dict
        raw_exif = record.raw_exif
        if isinstance(raw_exif, dict):
            raw_exif = json.dumps(raw_exif)

        with get_conn() as conn:
            conn.execute("""
                INSERT OR IGNORE INTO photo_metadata (
                    file_path,
                    filename,
                    file_extension,
                    file_size_bytes,
                    taken_at,
                    local_date,
                    date_created,
                    latitude,
                    longitude,
                    gps_source,
                    track_join_delta_s,
                    place_id,
                    media_type,
                    photo_type,
                    width,
                    height,
                    duration_s,
                    is_screenshot,
                    is_screen_recording,
                    is_favourite,
                    source_app,
                    camera_make,
                    camera_model,
                    album,
                    raw_exif
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                record.file_path,
                record.filename,
                record.file_extension,
                int(record.file_size_bytes) if record.file_size_bytes is not None else None,
                record.taken_at,
                record.local_date,
                record.date_created,
                record.latitude,
                record.longitude,
                record.gps_source,
                record.track_join_delta_s,
                record.place_id,
                record.media_type,
                record.photo_type,
                record.width,
                record.height,
                record.duration_s,
                int(record.is_screenshot),
                int(record.is_screen_recording),
                int(record.is_favourite),
                record.source_app,
                record.camera_make,
                record.camera_model,
                record.album,
                raw_exif,
            ))
            return conn.execute("SELECT changes()").fetchone()[0] > 0


table = PhotoMetadataTable()