import logging

from database.util import get_conn
from database.location.overland.util import _normalise_ts
from telemetry_models import OverlandPayload

logger = logging.getLogger(__name__)

def init():
    """Create the location_overland table and its indexes if they do not exist."""
    with get_conn() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS location_overland (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id           TEXT,
            timestamp           TEXT NOT NULL,          -- ISO 8601 UTC
            lat                 REAL NOT NULL,
            lon                 REAL NOT NULL,
            altitude            REAL,
            speed               REAL,                   -- m/s, NULL if unavailable
            horizontal_accuracy REAL,                   -- metres
            vertical_accuracy   REAL,                   -- metres, NULL if unavailable
            motion              TEXT,                   -- JSON array e.g. '["walking"]'
            activity            TEXT,
            wifi_ssid           TEXT,
            battery_state       TEXT,
            battery_level       REAL,
            pauses              INTEGER,                -- BOOLEAN as 0/1
            desired_accuracy    REAL,
            significant_change  TEXT,
            raw_json            TEXT,                   -- full original feature for safety
            inserted_at         TEXT DEFAULT (datetime('now')),
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

def insert_overland(payload: OverlandPayload, device_id: str):
    """Upsert all location points from an Overland payload into location_overland.

    Uses INSERT OR IGNORE against a UNIQUE(device_id, timestamp) constraint so
    duplicate payloads are safe to retry.  Logs a summary at UPLOAD level once
    the batch is complete.
    """
    inserted = 0
    skipped = 0
    import json
    
    with get_conn() as conn:

        for feature in payload.locations:
            geo = feature.geometry
            props = feature.properties

            lon, lat = geo.coordinates[0], geo.coordinates[1]

            try:
                conn.execute("""
                INSERT OR IGNORE INTO location_overland (
                            device_id, timestamp, lat, lon, altitude,
                            speed, horizontal_accuracy, vertical_accuracy,
                            motion, activity, wifi_ssid, 
                            battery_state, battery_level, 
                            pauses, desired_accuracy, significant_change, raw_json
                            ) VALUES (
                        :device_id, :timestamp, :lat, :lon, :altitude,
                    :speed, :horizontal_accuracy, :vertical_accuracy,
                        :motion, :activity, :wifi_ssid,
                        :battery_state, :battery_level,
                        :pauses, :desired_accuracy, :significant_change,
                        :raw_json
                    )
                    """,
                    {
                        "device_id":           device_id,
                        "timestamp":           _normalise_ts(props.timestamp),
                        "lat":                 lat,
                        "lon":                 lon,
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

    return {"result": "ok"}