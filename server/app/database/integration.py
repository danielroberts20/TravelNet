from database.util import get_conn
from telemetry_models import Log
from database.cellular.table import init as init_cellular, insert_cellular_state
from database.exchange.table import init as init_fx
from database.location.table import init as init_location, insert_location
from database.health.table import init as init_health
from database.transaction.table import init as init_transactions
from database.job.table import init as init_jobs

# -----------------------------
# Initialization
# -----------------------------
def init_db():
    init_cellular()
    init_fx()
    init_location()
    init_health()
    init_transactions()
    init_jobs()

# -----------------------------
# Insertion helpers
# -----------------------------
def insert_log(log: Log):
    if not validate_log(log):
        return
    with get_conn() as conn:
        location_id = insert_location(conn, log.timestamp, log.timezone, log.latitude, log.longitude, log.altitude,
                                      log.activity, log.device, log.is_locked, log.battery, 
                                      log.is_charging, log.is_connected_charger, log.BSSID, log.RSSI)
        
        insert_cellular_state(conn, log.cellular_states, location_id)
        conn.commit()

def validate_log(log: Log):
    #if True:
    #    return False
    return True
