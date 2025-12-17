import os
import sys
import requests
from pprint import pprint
from datetime import datetime
import pandas as pd

def get_weatherapi_current(api_key, city):
    url = "http://api.weatherapi.com/v1/current.json"
    params = {"key": api_key, "q": city}

    data = requests.get(url, params=params, timeout=10).json()

    current = data.get("current", {})

    wind_kph = current.get("wind_kph")
    wind_degree = current.get("wind_degree")
    wind_dir = current.get("wind_dir")

    # Timestamp UTC
    time = datetime.utcnow()

    return pd.DataFrame([{
        "time": pd.to_datetime(time, utc=True),
        "wind_kph": float(wind_kph) if wind_kph is not None else None,
        "wind_degree": int(wind_degree) if wind_degree is not None else None,
        "wind_dir": str(wind_dir) if wind_dir is not None else None
    }])


def get_weatherapi_last_24h(api_key, city):
    """
    Récupère les dernières 24h de vent, humidité et pression depuis WeatherAPI.
    Retourne un DataFrame tz-aware UTC.
    """
    # Dates UTC
    end = pd.Timestamp.utcnow()
    start = end - pd.Timedelta(hours=24)

    today = end.date().isoformat()
    yesterday = (end - pd.Timedelta(days=1)).date().isoformat()

    df_list = []

    for day in [yesterday, today]:
        url = "http://api.weatherapi.com/v1/history.json"
        params = {"key": api_key, "q": city, "dt": day}
        data = requests.get(url, params=params, timeout=10).json()

        hours = data.get("forecast", {}).get("forecastday", [])[0].get("hour", [])
        if not hours:
            continue

        df_day = pd.DataFrame([{
            "time": pd.to_datetime(h["time"], utc=True),
            "wind_kph": h["wind_kph"],
            "wind_degree": h["wind_degree"],
            "humidity": h.get("humidity", None),       # humidité %
            "pressure_mb": h.get("pressure_mb", None)  # pression atmosphérique
        } for h in hours])

        df_list.append(df_day)

    if not df_list:
        return pd.DataFrame(columns=["time", "wind_kph", "wind_degree", "humidity", "pressure_mb"])

    # Concat toutes les heures
    df = pd.concat(df_list, ignore_index=True)

    # Filtrer les dernières 24h exactes
    df = df[(df["time"] >= start) & (df["time"] < end)].reset_index(drop=True)

    return df
