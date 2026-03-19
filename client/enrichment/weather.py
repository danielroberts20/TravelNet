from datetime import date
import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry

from client.config.weather import WEATHER_METRICS

cache_session = requests_cache.CachedSession('.cache', expire_after=-1)  # cache forever — historical data never changes
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

def get_weather_data(lat: float, lon: float, on_date: date|str, metrics: list[str]|None = None, timezone: str = "auto"):
    if metrics is None:
        metrics = WEATHER_METRICS

    if isinstance(on_date, date):
        on_date = on_date.strftime("%Y-%m-%d")

    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": on_date,
        "end_date": on_date,
        "hourly": metrics,
        "timezone": timezone,
    }

    response = openmeteo.weather_api(
        "https://archive-api.open-meteo.com/v1/archive", params=params
    )[0]

    hourly = response.Hourly()
    timezone = response.Timezone().decode("utf-8")

    data = {
        "timestamp": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left",
        ).tz_convert(timezone),
    }

    for i, metric in enumerate(WEATHER_METRICS):
        data[metric] = hourly.Variables(i).ValuesAsNumpy()

    return pd.DataFrame(data)

# TODO -- Average location by hour, look up hour in weather response