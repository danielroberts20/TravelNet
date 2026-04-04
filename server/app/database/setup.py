from config.editable import log_config_summary
from database.connection import get_conn
from models.telemetry import Log
from database.places.table import init as init_places
from database.cellular.table import init as init_cellular, insert_cellular_state
from database.exchange.table import init as init_fx
from database.location.table import init as init_location, init_unified_view, insert_location
from database.health.table import init as init_health
from database.health.heart_rate.table import init as init_heart_rate
from database.health.sleep.table import init as init_sleep
from database.health.workouts.table import init as init_workouts
from database.health.mood.table import init as init_mood
from database.transaction.table import init as init_transactions
from database.triggers.table import init as init_triggers
from database.job.table import init as init_jobs
from database.location.overland.table import init as init_overland
from database.location.gap_annotations.table import init as init_gap_annotations
from database.weather.table import init as init_weather
from database.logging.table import init as init_log_digest
from database.ml.table import init as init_ml
from triggers.location_change import init as init_location_change

# -----------------------------
# Initialization
# -----------------------------
def init_db():
    """Initialise all DB tables and views.

    places must be created first as many other tables reference it via
    foreign keys.  The unified location view is created last because it
    depends on both location_shortcuts and location_overland.
    """
    init_places()        # Must be first — FK target for all place_id columns
    init_cellular()
    init_fx()
    init_location()
    init_health()
    init_heart_rate()
    init_sleep()
    init_workouts()
    init_mood()
    init_transactions()
    init_triggers()
    init_jobs()
    init_overland()
    init_gap_annotations()
    init_weather()
    init_log_digest()
    init_ml()
    init_location_change()
    log_config_summary()

    # Must be last — depends on location_shortcuts and location_overland tables
    init_unified_view()

# -----------------------------
# Insertion helpers
# -----------------------------
def insert_log(log: Log):
    """Insert a single Shortcuts telemetry log row (location + cellular state)."""
    with get_conn() as conn:
        location_id = insert_location(conn, log.timestamp, log.latitude, log.longitude, log.altitude,
                                      log.device, log.is_locked, log.battery,
                                      log.is_charging, log.is_connected_charger, log.BSSID, log.RSSI)

        insert_cellular_state(conn, log.cellular_states, location_id)
        conn.commit()
