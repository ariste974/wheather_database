from datetime import datetime, timedelta
import pandas as pd
import requests
from meteostat import Hourly, Stations
import openmeteo_requests
import requests_cache
from retry_requests import retry

from meteostat_api import get_meteostat_current, get_meteostat_last_24h
from openmeteo_api import get_openmeteo_current, get_openmeteo_last_24h
from wheather_api import get_weatherapi_current, get_weatherapi_last_24h

from connect_db import insert_data

from dotenv import load_dotenv
import os

load_dotenv()
# --------------------------------------------
# PARAMÈTRES
# --------------------------------------------
# Coordonnées et ville pour Riga



list_ville=[
  {"city":"Riga","country":"Latvia","lat":56.9496,"lon":24.1052, "station": None},
  {"city":"Paris","country":"France","lat":48.8566,"lon":2.3522, "station": None},
  {"city":"Berlin","country":"Germany","lat":52.52,"lon":13.405, "station": None},
  {"city":"London","country":"UK","lat":51.5074,"lon":-0.1278, "station": None}
]
for ville in list_ville:
    CITY=ville["city"]
    LAT=ville["lat"]
    LON=ville["lon"]
    # Recherche de la station Meteostat la plus proche automatiquement
    stations = Stations()
    near = stations.nearby(LAT, LON).fetch(1)

    if not near.empty:
        # L'identifiant de station est l'index du DataFrame retourné
        ville["station"] = str(near.index[0])
    
WEATHERAPI_KEY = os.getenv("API_KEY")


# ===============================================================
# PIPELINE 
# ===============================================================
def get_current_weather(LAT, LON, CITY, STATION):
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
    row=df.iloc[0]
    print("Data fetched successfully:", df)

    insert_data(row, LAT, LON)
    
    return "Data inserted into the database."



def get_weather_last_24h(LAT, LON, CITY, STATION):
    print("Fetching Open-Meteo (last 24h)...")
    df_open = get_openmeteo_last_24h(LAT, LON)
    
    print("Fetching Meteostat (last 24h)...")
    df_rain = get_meteostat_last_24h(STATION)
    
    print("Fetching WeatherAPI (last 24h)...")
    df_wind = get_weatherapi_last_24h(WEATHERAPI_KEY, CITY)
    
    df_open["time"] = pd.to_datetime(df_open["time"], utc=True)
    df_rain["time"] = pd.to_datetime(df_rain["time"], utc=True)
    df_wind["time"] = pd.to_datetime(df_wind["time"], utc=True)

    # 🔗 Fusion horaire
    df = (
        df_open
        .merge(df_rain, on="time", how="left")
        .merge(df_wind, on="time", how="left")
        .sort_values("time")
    )

    if df.empty:
        print("No data fetched.")
        return "No data"

    print(f"{len(df)} rows fetched")

    # 🧠 Insertion ligne par ligne
    for _, row in df.iterrows():
        insert_data(row, LAT, LON)

    print("Data successfully inserted into the database.")

    return df



# ===============================================================
# MAIN
# ===============================================================
if __name__ == "__main__":
    for ville in list_ville:
        LAT=ville["lat"]
        LON=ville["lon"]
        CITY=ville["city"]
        STATION=ville["station"]
        print(f"\n===== Traitement pour la ville de {CITY} =====")
        result = get_weather_last_24h(LAT, LON, CITY, STATION)
        print(result)
        
    
    
