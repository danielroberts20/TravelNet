from datetime import date

from client.config.weather import WEATHER_METRICS

cache_session = requests_cache.CachedSession('.cache', expire_after=-1)  # cache forever — historical data never changes
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

def get_weather_data(lat: float, lon: float, on_date: date, metrics: list[str]|None = None):
    if metrics is None:
        metrics = WEATHER_METRICS
    
    pass