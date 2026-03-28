from config.editable import log_config_summary
from database.util import get_conn
from telemetry_models import Log
from database.cellular.table import init as init_cellular, insert_cellular_state
from database.exchange.table import init as init_fx
from database.location.table import init as init_location, init_unified_view, insert_location
from database.health.table import init as init_health
from database.health.workouts.table import init as init_workouts
from database.transaction.table import init as init_transactions
from database.job.table import init as init_jobs
from database.location.overland.table import init as init_overland
from database.location.gap_annotations.table import init as init_gap_annotations
from database.weather.table import init as init_weather

# -----------------------------
# Initialization
# -----------------------------
def init_db():
    """Initialise all DB tables and views.

    Creates tables for cellular, FX, location, health, workouts, transactions,
    jobs, Overland GPS, gap annotations, and weather data.  Must be called once
    at startup before any insert helpers are used.  The unified location view is
    created last because it depends on both location_shortcuts and location_overland.
    """
    init_cellular()
    init_fx()
    init_location()
    init_health()
    init_workouts()
    init_transactions()
    init_jobs()
    init_overland()
    init_gap_annotations()
    init_weather()
    log_config_summary()

    # Must be last — depends on location_shortcuts and location_overland tables
    init_unified_view()

# -----------------------------
# Insertion helpers
# -----------------------------
def insert_log(log: Log):
    """Insert a single Shortcuts telemetry log row (location + cellular state)."""
    if not validate_log(log):
        return
    with get_conn() as conn:
        location_id = insert_location(conn, log.timestamp, log.latitude, log.longitude, log.altitude,
                                      log.activity, log.device, log.is_locked, log.battery,
                                      log.is_charging, log.is_connected_charger, log.BSSID, log.RSSI)

        insert_cellular_state(conn, log.cellular_states, location_id)
        conn.commit()

def validate_log(log: Log):
    """Return True if the log entry should be inserted, False to skip it."""
    return True
