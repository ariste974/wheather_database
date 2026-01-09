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
    Récupère les dernières 7 jours d'heures (UTC) depuis WeatherAPI.
    Retourne un DataFrame tz-aware UTC.
    """
    end = pd.Timestamp.utcnow()
    start = end - pd.Timedelta(hours=168)  # 7 jours

    df_list = []

    for day in pd.date_range(start.normalize(), end.normalize(), freq="D"):
        url = "http://api.weatherapi.com/v1/history.json"
        params = {"key": api_key, "q": city, "dt": day.date().isoformat()}
        try:
            data = requests.get(url, params=params, timeout=10).json()
        except Exception:
            continue

        forecast_days = data.get("forecast", {}).get("forecastday", [])
        if not forecast_days:
            continue

        hours = forecast_days[0].get("hour", [])
        if not hours:
            continue

        df_day = pd.DataFrame([{
            "time": pd.to_datetime(h.get("time"), utc=True),
            "wind_kph": h.get("wind_kph"),
            "wind_degree": h.get("wind_degree"),
            "humidity": h.get("humidity", None),
            "pressure_mb": h.get("pressure_mb", None)
        } for h in hours])

        df_list.append(df_day)

    if not df_list:
        return pd.DataFrame(columns=["time", "wind_kph", "wind_degree", "humidity", "pressure_mb"])

    df = pd.concat(df_list, ignore_index=True)
    df = df.dropna(subset=["time"])
    df = df.sort_values("time").reset_index(drop=True)

    # Filtrer uniquement l'intervalle exact demandé
    df = df[(df["time"] >= start) & (df["time"] < end)].reset_index(drop=True)

    return df

