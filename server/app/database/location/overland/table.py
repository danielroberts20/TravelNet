"""
database/location/overland/table.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Schema and insert helpers for the location_overland table (Overland JSONL path).

insert() processes a full OverlandPayload, unpacking each GeoJSON feature into
a row. Duplicate points are silently ignored via UNIQUE(device_id, timestamp).
The caller (upload/location/router.py) is responsible for firing location
triggers after insert returns.
"""

import json
import logging
from dataclasses import dataclass
from typing import Optional

from config.general import LOCATION_NOISE_ACCURACY_THRESHOLD
from database.location.noise.table import table as noise_table, LocationNoiseRecord
from database.base import BaseTable
from database.connection import get_conn, to_iso_str
from database.location.overland.util import _normalise_ts
from models.telemetry import OverlandPayload

logger = logging.getLogger(__name__)


@dataclass
class OverlandRecord:
    device_id: str
    timestamp: str          # ISO 8601 UTC — already normalised by caller
    latitude: float
    longitude: float
    place_id: Optional[int] = None
    altitude: Optional[float] = None
    speed: Optional[float] = None
    horizontal_accuracy: Optional[float] = None
    vertical_accuracy: Optional[float] = None
    motion: Optional[str] = None
    activity: Optional[str] = None
    wifi_ssid: Optional[str] = None
    battery_state: Optional[str] = None
    battery_level: Optional[float] = None
    pauses: Optional[int] = None
    desired_accuracy: Optional[float] = None
    significant_change: Optional[str] = None
    raw_json: Optional[str] = None


class LocationOverlandTable(BaseTable[OverlandRecord]):

    def init(self) -> None:
        """Create the location_overland table and its indexes if they do not exist."""
        with get_conn() as conn:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS location_overland (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id           TEXT NOT NULL,
                timestamp           TEXT NOT NULL,
                latitude            REAL NOT NULL CHECK(latitude  BETWEEN -90  AND 90),
                longitude           REAL NOT NULL CHECK(longitude BETWEEN -180 AND 180),
                altitude            REAL,
                speed               REAL,
                horizontal_accuracy REAL,
                vertical_accuracy   REAL,
                motion              TEXT,
                activity            TEXT,
                wifi_ssid           TEXT,
                battery_state       TEXT,
                battery_level       REAL,
                pauses              INTEGER,
                desired_accuracy    REAL,
                significant_change  TEXT,
                place_id            INTEGER REFERENCES places(id),
                raw_json            TEXT,
                inserted_at         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
                UNIQUE(device_id, timestamp)
            );""")

            conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_overland_timestamp
                ON location_overland(timestamp);
            """)

            conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_overland_device_ts
                ON location_overland(device_id, timestamp);
            """)

            conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_overland_lat_lon
                ON location_overland(latitude, longitude);
            """)

            conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_overland_place
                ON location_overland(place_id);
            """)

    def insert(self, record: OverlandRecord) -> None:
        """Insert a single Overland location row. Idempotent on (device_id, timestamp)."""
        with get_conn() as conn:
            conn.execute("""
                INSERT OR IGNORE INTO location_overland (
                    device_id, timestamp, latitude, longitude, altitude,
                    speed, horizontal_accuracy, vertical_accuracy,
                    motion, activity, wifi_ssid,
                    battery_state, battery_level,
                    pauses, desired_accuracy, significant_change, raw_json, place_id
                ) VALUES (
                    :device_id, :timestamp, :latitude, :longitude, :altitude,
                    :speed, :horizontal_accuracy, :vertical_accuracy,
                    :motion, :activity, :wifi_ssid,
                    :battery_state, :battery_level,
                    :pauses, :desired_accuracy, :significant_change,
                    :raw_json, :place_id
                )
            """, {
                "device_id":           record.device_id,
                "timestamp":           record.timestamp,
                "latitude":            record.latitude,
                "longitude":           record.longitude,
                "altitude":            record.altitude,
                "speed":               record.speed,
                "horizontal_accuracy": record.horizontal_accuracy,
                "vertical_accuracy":   record.vertical_accuracy,
                "motion":              record.motion,
                "activity":            record.activity,
                "wifi_ssid":           record.wifi_ssid,
                "battery_state":       record.battery_state,
                "battery_level":       record.battery_level,
                "pauses":              record.pauses,
                "desired_accuracy":    record.desired_accuracy,
                "significant_change":  record.significant_change,
                "raw_json":            record.raw_json,
                "place_id":            record.place_id,
            })

    def insert_payload(self, payload: OverlandPayload, device_id: str) -> tuple[int, int]:
        """Insert all location points from an Overland payload.

        Unpacks each GeoJSON feature, resolves a place_id via the places table,
        and inserts into location_overland. Returns (inserted, skipped) counts.
        The caller is responsible for firing location triggers after this returns.

        Noise-flagging (location_noise inserts) is deferred until after the main
        transaction commits.  Calling noise_table.insert() inside the outer
        `with get_conn()` block would open a second write connection while the
        first still holds the WAL write lock, causing "database is locked".
        """
        inserted = 0
        skipped = 0
        pending_noise: list[tuple[int, float, float, str, str]] = []  # (overland_id, h_acc, lat, lon, ts)

        with get_conn() as conn:
            for feature in payload.locations:
                geo = feature.geometry
                props = feature.properties

                lon, lat = geo.coordinates[0], geo.coordinates[1]
                ts = to_iso_str(_normalise_ts(props.timestamp))
                lat_snap, lon_snap = round(lat, 3), round(lon, 3)

                conn.execute(
                    "INSERT OR IGNORE INTO places (lat_snap, lon_snap) VALUES (?, ?)",
                    (lat_snap, lon_snap),
                )
                row = conn.execute(
                    "SELECT id FROM places WHERE lat_snap = ? AND lon_snap = ?",
                    (lat_snap, lon_snap),
                ).fetchone()
                place_id = row[0] if row else None

                try:
                    cursor = conn.execute("""
                        INSERT OR IGNORE INTO location_overland (
                            device_id, timestamp, latitude, longitude, altitude,
                            speed, horizontal_accuracy, vertical_accuracy,
                            motion, activity, wifi_ssid,
                            battery_state, battery_level,
                            pauses, desired_accuracy, significant_change, raw_json, place_id
                        ) VALUES (
                            :device_id, :timestamp, :latitude, :longitude, :altitude,
                            :speed, :horizontal_accuracy, :vertical_accuracy,
                            :motion, :activity, :wifi_ssid,
                            :battery_state, :battery_level,
                            :pauses, :desired_accuracy, :significant_change,
                            :raw_json, :place_id
                        )
                    """, {
                        "device_id":           device_id,
                        "timestamp":           ts,
                        "latitude":            lat,
                        "longitude":           lon,
                        "altitude":            props.altitude,
                        "speed":               props.speed,
                        "horizontal_accuracy": props.horizontal_accuracy,
                        "vertical_accuracy":   props.vertical_accuracy,
                        "motion":              json.dumps(props.motion) if props.motion else None,
                        "activity":            props.activity,
                        "wifi_ssid":           props.wifi or None,
                        "battery_state":       props.battery_state,
                        "battery_level":       props.battery_level,
                        "pauses":              int(props.pauses) if props.pauses is not None else None,
                        "desired_accuracy":    props.desired_accuracy,
                        "significant_change":  props.significant_change,
                        "raw_json":            json.dumps(feature.model_dump()),
                        "place_id":            place_id,
                    })

                    if cursor.rowcount:
                        inserted += 1
                        overland_id = cursor.lastrowid
                    else:
                        skipped += 1
                        row = conn.execute(
                            "SELECT id FROM location_overland WHERE device_id = ? AND timestamp = ?",
                            (device_id, ts),
                        ).fetchone()
                        overland_id = row[0] if row else None

                    if overland_id and props.horizontal_accuracy and props.horizontal_accuracy > LOCATION_NOISE_ACCURACY_THRESHOLD:
                        pending_noise.append((overland_id, props.horizontal_accuracy, lat, lon, ts))

                except Exception as e:
                    logger.error(f"Failed to insert Overland point {props.timestamp}: {e}")

            conn.commit()

        # Outer transaction committed — write lock released.  Now safe to open a
        # second connection for noise inserts.
        for overland_id, h_acc, lat, lon, ts in pending_noise:
            noise_table.insert(LocationNoiseRecord(overland_id, tier=1, reason='accuracy_threshold'))
            logger.important(
                f"High accuracy value ({h_acc}m) for point at "
                f"{ts} ({lat}, {lon}). Inserted to noise table as tier 1 noise."
            )

        logger.info(
            f"Overland batch: {len(payload.locations)} received, "
            f"{inserted} inserted, {skipped} skipped (duplicates/errors)"
        )

        return inserted, skipped


table = LocationOverlandTable()
