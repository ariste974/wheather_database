from datetime import datetime, timedelta
import pandas as pd
import requests
from meteostat import Hourly
import openmeteo_requests
import requests_cache
from retry_requests import retry

from meteostat_api import get_meteostat_current
from openmeteo_api import get_openmeteo_current
from wheather_api import get_weatherapi_current

from dotenv import load_dotenv
import os

load_dotenv()
# --------------------------------------------
# PARAMÈTRES
# --------------------------------------------
LAT = 52.52
LON = 13.41
STATION = "72219"      # Meteostat station
CITY = "Berlin"
WEATHERAPI_KEY = os.getenv("API_KEY")


# ===============================================================
# PIPELINE 
# ===============================================================
def get_current_weather():
    print("Fetching Open-Meteo...")
    open_data = get_openmeteo_current(LAT, LON)

    print("Fetching Meteostat...")
    meteo_data = get_meteostat_current(STATION)

    print("Fetching WeatherAPI...")
    wind_data = get_weatherapi_current(WEATHERAPI_KEY, CITY)

    # Fusion en un seul record
    all_data = {**open_data, **meteo_data, **wind_data}
    all_data["timestamp_utc"] = datetime.utcnow()

    df = pd.DataFrame([all_data])
    return df


# ===============================================================
# MAIN
# ===============================================================
if __name__ == "__main__":
    df = get_current_weather()
    print("\n===== Données météo de maintenant =====")
    print(df)
