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


sensor_id={"temperature_2m":"SENS-001","prcp":"SENS-002","wind_kph":"SENS-003","wind_degree":"SENS-004","humidity":"SENS-005","pressure_mb":"SENS-006"}
list_ville = [
    # Déjà existantes
    {"location_id": "LV-1000", "city": "Riga", "country": "Latvia", "lat": 56.9496, "lon": 24.1052, "station": None, "device_id": "RIGA001"},
    {"location_id": "FR-75000", "city": "Paris", "country": "France", "lat": 48.8566, "lon": 2.3522, "station": None, "device_id": "PARIS001"},
    {"location_id": "DE-1000", "city": "Berlin", "country": "Germany", "lat": 52.52, "lon": 13.405, "station": None, "device_id": "BERLIN001"},
    {"location_id": "UK-1000", "city": "London", "country": "UK", "lat": 51.5074, "lon": -0.1278, "station": None, "device_id": "LONDON001"},

    # Lettonie
    {"location_id": "LV-2000", "city": "Daugavpils", "country": "Latvia", "lat": 55.8758, "lon": 26.5358, "station": None, "device_id": "DAUGAVPILS001"},
    {"location_id": "LV-3000", "city": "Liepāja", "country": "Latvia", "lat": 56.5047, "lon": 21.0108, "station": None, "device_id": "LIEPAJA001"},
    {"location_id": "LV-4000", "city": "Jelgava", "country": "Latvia", "lat": 56.6510, "lon": 23.7214, "station": None, "device_id": "JELGAVA001"},

    # Lituanie
    {"location_id": "LT-1000", "city": "Vilnius", "country": "Lithuania", "lat": 54.6872, "lon": 25.2797, "station": None, "device_id": "VILNIUS001"},
    {"location_id": "LT-2000", "city": "Kaunas", "country": "Lithuania", "lat": 54.8985, "lon": 23.9036, "station": None, "device_id": "KAUNAS001"},
    {"location_id": "LT-3000", "city": "Klaipėda", "country": "Lithuania", "lat": 55.7033, "lon": 21.1443, "station": None, "device_id": "KLAIPEDA001"},

    # Estonie
    {"location_id": "EE-1000", "city": "Tallinn", "country": "Estonia", "lat": 59.4370, "lon": 24.7536, "station": None, "device_id": "TALLINN001"},
    {"location_id": "EE-2000", "city": "Tartu", "country": "Estonia", "lat": 58.3776, "lon": 26.7290, "station": None, "device_id": "TARTU001"},
    {"location_id": "EE-3000", "city": "Narva", "country": "Estonia", "lat": 59.3790, "lon": 28.1903, "station": None, "device_id": "NARVA001"},

    # Autres villes européennes (pour arriver à 20)
    {"location_id": "ES-1000", "city": "Madrid", "country": "Spain", "lat": 40.4168, "lon": -3.7038, "station": None, "device_id": "MADRID001"},
    {"location_id": "IT-1000", "city": "Rome", "country": "Italy", "lat": 41.9028, "lon": 12.4964, "station": None, "device_id": "ROME001"},
    {"location_id": "NL-1000", "city": "Amsterdam", "country": "Netherlands", "lat": 52.3676, "lon": 4.9041, "station": None, "device_id": "AMSTERDAM001"},
    {"location_id": "BE-1000", "city": "Brussels", "country": "Belgium", "lat": 50.8503, "lon": 4.3517, "station": None, "device_id": "BRUSSELS001"},
    {"location_id": "SE-1000", "city": "Stockholm", "country": "Sweden", "lat": 59.3293, "lon": 18.0686, "station": None, "device_id": "STOCKHOLM001"},
    {"location_id": "FI-1000", "city": "Helsinki", "country": "Finland", "lat": 60.1699, "lon": 24.9384, "station": None, "device_id": "HELSINKI001"}
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



def get_weather_last_24h(LAT, LON, CITY, STATION,device_id=None):
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
        row["device_id"]=device_id
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
        result = get_weather_last_24h(LAT, LON, CITY, STATION,ville["device_id"])
        print(result)
        
    
    
