"""
database/setup.py
~~~~~~~~~~~~~~~~~
Database initialisation entry point.

TABLE_REGISTRY lists every BaseTable instance in dependency order:
- places must be first (FK target for all place_id columns)
- location_overland and location_shortcuts must precede the unified view
- known_places must precede place_visits (FK)
- The unified view is initialised last via location_table.init_unified_view()
"""
from config.editable import log_config_summary
from database.base import BaseTable
from models.telemetry import Log


from database.connection import get_conn
from database.places.table import table as places_table
from database.cellular.table import table as cellular_table, CellularRecord
from database.exchange.table import table as fx_table
from database.location.table import table as location_table, LocationRecord
from database.transition.timezone.table import table as transition_timezone_table
from database.transition.country.table import table as transition_country_table
from database.health.table import table as health_table
from database.flights.table import table as flights_table
from database.health.heart_rate.table import table as heart_rate_table
from database.health.sleep.table import table as sleep_table
from database.health.workouts.table import table as workouts_table
from database.health.mood.table import table as mood_table
from database.transaction.table import table as transactions_table
from database.triggers.table import table as trigger_table
from database.location.overland.table import table as overland_table
from database.location.gap_annotations.table import table as gap_annotations_table
from database.location.known_places.table import table as known_places_table
from database.location.noise.table import table as noise_table
from database.weather.table import table as weather_table
from database.logging.digest.table import table as log_digest_table
from database.logging.daily.table import table as daily_cron_table
from database.ml.table import table as ml_table
from database.ml.location_segments import table as ml_location_segments_table
from database.ml.day_embeddings import table as ml_day_embeddings_table
from database.ml.destination_profiles import table as ml_destination_profiles_table
from database.ml.causal_graph import table as ml_causal_graph_table
from database.watchdog.table import table as watchdog_table
from database.power.table import table as power_table
from database.photos.table import table as photo_table
from database.summary.table import table as daily_summary_table
from database.cost_of_living.table import table as cost_of_living_table

TABLE_REGISTRY: list[BaseTable] = [
    places_table,           # Must be first — FK target for all place_id columns
    cellular_table,
    fx_table,
    location_table,         # location_shortcuts
    health_table,
    heart_rate_table,
    sleep_table,
    workouts_table,
    flights_table,
    mood_table,
    transactions_table,
    trigger_table,
    overland_table,
    gap_annotations_table,
    known_places_table,     # known_places + place_visits (must follow places_table)
    weather_table,
    log_digest_table,
    daily_cron_table,
    ml_table,
    ml_location_segments_table,
    ml_day_embeddings_table,
    ml_destination_profiles_table,
    ml_causal_graph_table,
    noise_table,
    transition_timezone_table,
    transition_country_table,
    watchdog_table,
    power_table,
    photo_table,
    daily_summary_table,
    cost_of_living_table,
]


def init_db() -> None:
    """Initialise all DB tables and views.

    Calls init() on every table in TABLE_REGISTRY, then creates the unified
    location view (which depends on both location_shortcuts and location_overland).
    """
    for t in TABLE_REGISTRY:
        t.init()

    # Must be last — depends on location_shortcuts and location_overland tables
    location_table.init_unified_view()
    noise_table.init_clean_view()
    daily_summary_table.init_complete_view()

    log_config_summary()

    # In transition_timezone table init(), after CREATE TABLE:
    with get_conn() as conn:
        conn.execute("""
            INSERT OR IGNORE INTO transition_timezone 
                (transitioned_at, from_tz, to_tz, from_offset, to_offset)
            VALUES ('2020-01-01T00:00:00Z', NULL, 'Europe/London', NULL, '+01:00')
        """)


def insert_log(log: Log) -> None:
    """Insert a single Shortcuts telemetry log row (location + cellular state)."""
    location_id = location_table.insert(LocationRecord(
        timestamp=log.timestamp,
        latitude=log.latitude,
        longitude=log.longitude,
        device=log.device,
        altitude=log.altitude,
        is_locked=log.is_locked,
        battery=log.battery,
        is_charging=log.is_charging,
        is_connected_charger=log.is_connected_charger,
        bssid=log.BSSID,
        rssi=log.RSSI,
    ))
    cellular_table.insert_batch(log.cellular_states, location_id)
