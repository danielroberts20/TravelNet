import logging
import json

from database.connection import get_conn, to_iso_str
from database.location.overland.util import _normalise_ts
from models.telemetry import OverlandPayload
from triggers import location_change


logger = logging.getLogger(__name__)

def init():
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

def insert_overland(payload: OverlandPayload, device_id: str):
    """Upsert all location points from an Overland payload into location_overland.

    Uses INSERT OR IGNORE against a UNIQUE(device_id, timestamp) constraint so
    duplicate payloads are safe to retry.  Logs a summary at UPLOAD level once
    the batch is complete.
    """
    inserted = 0
    skipped = 0
    
    with get_conn() as conn:

        for feature in payload.locations:
            geo = feature.geometry
            props = feature.properties

            lon, lat = geo.coordinates[0], geo.coordinates[1]
            new_ts = to_iso_str(_normalise_ts(props.timestamp))
            lat_snap, lon_snap = round(lat, 3), round(lon, 3)
            conn.execute("""
                INSERT OR IGNORE INTO places (lat_snap, lon_snap) VALUES (?, ?)
                """, (lat_snap, lon_snap))
            row = conn.execute("""
                SELECT id FROM places WHERE lat_snap = ? AND lon_snap = ?
                """, (lat_snap, lon_snap)).fetchone()
            place_id = row[0] if row else None

            try:
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
                    """,
                    {
                        "device_id":           device_id,
                        "timestamp":           new_ts,
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
                        "place_id":            place_id
                    },
                )

                if conn.execute("SELECT changes()").fetchone()[0]:
                    inserted += 1
                else:
                    skipped += 1
            except Exception as e:
                logger.error(f"Failed to insert Overland point {props.timestamp}: {e}")
        
        conn.commit()

    logger.upload(
        f"Overland batch: {len(payload.locations)} received, "
        + f"{inserted} inserted, {skipped} skipped (duplicates/errors)")
    
    location_change.run()

    return {"result": "ok"}