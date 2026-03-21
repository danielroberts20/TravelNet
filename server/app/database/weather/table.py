from config.general import COORD_PRECISION
from database.util import get_conn
import logging

logger = logging.getLogger(__name__)


from database.util import get_conn
import logging

logger = logging.getLogger(__name__)


def init():
    with get_conn() as conn:
        try:
            conn.executescript(f"""
                CREATE TABLE IF NOT EXISTS weather (
                    id               INTEGER PRIMARY KEY AUTOINCREMENT,
                    fetched_at       DATETIME NOT NULL,
                    timestamp        DATETIME NOT NULL,
                    latitude         REAL NOT NULL,
                    longitude        REAL NOT NULL,
                    temperature_c    REAL,
                    precipitation_mm REAL,
                    windspeed_kmh    REAL,
                    weathercode      INTEGER,
                    raw_json         TEXT,
                    UNIQUE(timestamp, latitude, longitude)
                );

                CREATE INDEX IF NOT EXISTS idx_weather_timestamp
                    ON weather(timestamp);

                CREATE INDEX IF NOT EXISTS idx_weather_lat_lon
                    ON weather(latitude, longitude);

                CREATE VIEW IF NOT EXISTS location_weather AS
                SELECT
                    u.*,
                    w.temperature_c,
                    w.precipitation_mm,
                    w.windspeed_kmh,
                    w.weathercode,
                    w.fetched_at AS weather_fetched_at
                FROM location_unified u
                LEFT JOIN weather w
                    ON  ROUND(u.lat, {COORD_PRECISION}) = w.latitude
                    AND ROUND(u.lon, {COORD_PRECISION}) = w.longitude
                    AND strftime('%Y-%m-%dT%H:00', u.timestamp) = w.timestamp;
            """)
            logger.info("Migration complete: weather table + location_weather view created.")
        except Exception:
            logger.exception("Migration failed.")
            raise