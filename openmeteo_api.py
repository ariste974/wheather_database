import openmeteo_requests

import pandas as pd
import requests_cache
from retry_requests import retry

from datetime import datetime, timedelta

def get_openmeteo_current(lat, lon):
    cache_session = requests_cache.CachedSession(".cache", expire_after=300)
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

    # Timestamp Open-Meteo (UTC)
    time = pd.to_datetime(current.Time(), unit="s", utc=True)

    temperature = current.Variables(0).Value()
    sunshine = current.Variables(1).Value()

    return pd.DataFrame([{
        "time": time,
        "temperature_2m": float(temperature) if temperature is not None else None,
        "sunshine_duration_s": float(sunshine) if sunshine is not None else None
    }])


def get_openmeteo_last_24h(lat, lon):
    """
    Récupère les 24 dernières heures de température et ensoleillement depuis Open-Meteo (API historique).
    Retourne un DataFrame avec time (UTC), temperature_2m, sunshine_duration_s
    """
    # Sessions avec cache et retry
    cache_session = requests_cache.CachedSession('.cache', expire_after=300)
    retry_session = retry(cache_session, retries=3, backoff_factor=0.2)
    client = openmeteo_requests.Client(session=retry_session)

    # Définir la période des dernières 24h
    end = pd.Timestamp.utcnow()
    start = end - pd.Timedelta(hours=168)
    print(f"Fetching Open-Meteo data from {start} to {end} for lat={lat}, lon={lon}")
    # Open-Meteo Historical API
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start.date().isoformat(),
        "end_date": end.date().isoformat(),
        "hourly": ["temperature_2m"],
        "timezone": "UTC"
    }

    response = client.weather_api(url, params=params)[0]
    hourly = response.Hourly()

    df = pd.DataFrame()
    # Extraction des données
    df["time"] = pd.DataFrame({"date": pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left"
    )})

    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    df["temperature_2m"] = hourly_temperature_2m
    
    # Filtrer exactement les dernières 24h
    df = df[(df["time"] >= start) & (df["time"] < end)].reset_index(drop=True)

    # Convertir NaN en None si besoin pour SQL
    df = df.where(pd.notna(df), None)

    return df