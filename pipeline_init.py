from datetime import datetime, timedelta
import pandas as pd
import requests
from meteostat import Hourly, Stations

import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()
host="16.171.133.144"

sensor_id=[{"temperature_2m":"SENS-001", "sensor_name":"temperature_2m","metric":"temperature_2m","unit":"°C","device_id":["RIGA001","PARIS001","BERLIN001","LONDON001"]},{"prcp":"SENS-002", "sensor_name":"prcp","metric":"prcp","unit":"mm","device_id":["RIGA001","PARIS001","BERLIN001","LONDON001"]},{"wind_kph":"SENS-003", "sensor_name":"wind_kph","metric":"wind_kph","unit":"km/h","device_id":["RIGA001","PARIS001","BERLIN001","LONDON001"]},{"wind_degree":"SENS-004", "sensor_name":"wind_degree","metric":"wind_degree","unit":"°","device_id":["RIGA001","PARIS001","BERLIN001","LONDON001"]},{"humidity":"SENS-005", "sensor_name":"humidity","metric":"humidity","unit":"%","device_id":["RIGA001","PARIS001","BERLIN001","LONDON001"]},{"pressure_mb":"SENS-006", "sensor_name":"pressure_mb","metric":"pressure_mb","unit":"mb","device_id": ["RIGA001","PARIS001","BERLIN001","LONDON001"]}]
list_ville=[
  {"location_id":"LV-1000","city":"Riga","country":"Latvia","lat":56.9496,"lon":24.1052, "station": None,"device_id": "RIGA001"},
  {"location_id":"FR-75000","city":"Paris","country":"France","lat":48.8566,"lon":2.3522, "station": None, "device_id": "PARIS001"},
  {"location_id":"DE-1000","city":"Berlin","country":"Germany","lat":52.52,"lon":13.405, "station": None, "device_id": "BERLIN001"},
  {"location_id":"UK-1000","city":"London","country":"UK","lat":51.5074,"lon":-0.1278, "station": None, "device_id": "LONDON001"}
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
    

MDP_DADABASE = os.getenv("MDP_DADABASE")

query_device = """INSERT INTO devices (device_id, location_id, device_name, is_active) VALUES (%s, %s, %s, %s); """

def insert_data_device(row, lat, lon):
    conn = psycopg2.connect(
        host=host,
        port=5432,
        dbname="weatherdb",
        user="weatheruser",
        password=MDP_DADABASE
    )

    cursor = conn.cursor()

    cursor.execute(query_device, (
        row["device_id"],
        row["location_id"],
        row["device_id"],
        True
    ))

    conn.commit()
    cursor.close()
    conn.close()

for ville in list_ville:
    insert_data_device(ville, ville["lat"], ville["lon"])

query_location = """INSERT INTO locations (location_id,city_name,country_name,location) VALUES (%s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326)); """

def insert_data_location(row, lat, lon):
    conn = psycopg2.connect(
        host=host,
        port=5432,
        dbname="weatherdb",
        user="weatheruser",
        password=MDP_DADABASE
    )

    cursor = conn.cursor()

    cursor.execute(query_location, (
        row["location_id"],
        row["city"],
        row["country"],
        lat,
        lon
    ))

    conn.commit()
    cursor.close()
    conn.close()

for ville in list_ville:
    insert_data_location(ville, ville["lat"], ville["lon"])

query_sensors = """INSERT INTO sensors ( sensor_id, device_id, sensor_name, metric, unit) VALUES (%s, %s, %s, %s, %s); """

def insert_data_sensors(row, lat, lon):
    conn = psycopg2.connect(
        host=host,
        port=5432,
        dbname="weatherdb",
        user="weatheruser",
        password=MDP_DADABASE
    )

    cursor = conn.cursor()

    cursor.execute(query_sensors, (
        row["sensor_id"],
        row["device_id"],
        row["sensor_name"],
        row["metric"],
        row["unit"]
    ))

    conn.commit()
    cursor.close()
    conn.close()

for ville in list_ville:
    for sensor in sensor_id:
        row=sensor.copy()
        row["device_id"]=ville["device_id"]
        insert_data_sensors(row, ville["lat"], ville["lon"])