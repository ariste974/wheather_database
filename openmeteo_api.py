import openmeteo_requests

import pandas as pd
import requests_cache
from retry_requests import retry

def get_openmeteo_current(lat, lon):
    cache_session = requests_cache.CachedSession('.cache', expire_after=300)
    retry_session = retry(cache_session, retries=3, backoff_factor=0.2)
    client = openmeteo_requests.Client(session=retry_session)

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": ["temperature_2m", "sunshine_duration"]
    }

    response = client.weather_api(url, params=params)[0]
    current = response.Current()

    return {
        "temperature_2m": current.Variables(0).Value(),
        "sunshine_duration_s": current.Variables(1).Value()
    }