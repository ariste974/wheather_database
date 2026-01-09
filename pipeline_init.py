from datetime import datetime, timedelta
import pandas as pd
import requests
from meteostat import Hourly, Stations

import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()
host="51.20.31.180"

sensor_id = [
    {
        "sensor_id": "SENS-001",
        "sensor_name": "temperature_2m",
        "metric": "temperature",
        "unit": "°C",
        "device_id": [
            "RIGA001","PARIS001","BERLIN001","LONDON001",
            "DAUGAVPILS001","LIEPAJA001","JELGAVA001",
            "VILNIUS001","KAUNAS001","KLAIPEDA001",
            "TALLINN001","TARTU001","NARVA001",
            "MADRID001","ROME001","AMSTERDAM001",
            "BRUSSELS001","STOCKHOLM001","HELSINKI001"
        ]
    },
    {
        "sensor_id": "SENS-002",
        "sensor_name": "prcp",
        "metric": "rainfall",
        "unit": "mm",
        "device_id": [
            "RIGA001","PARIS001","BERLIN001","LONDON001",
            "DAUGAVPILS001","LIEPAJA001","JELGAVA001",
            "VILNIUS001","KAUNAS001","KLAIPEDA001",
            "TALLINN001","TARTU001","NARVA001",
            "MADRID001","ROME001","AMSTERDAM001",
            "BRUSSELS001","STOCKHOLM001","HELSINKI001"
        ]
    },
    {
        "sensor_id": "SENS-003",
        "sensor_name": "wind_kph",
        "metric": "wind_speed",
        "unit": "km/h",
        "device_id": [
            "RIGA001","PARIS001","BERLIN001","LONDON001",
            "DAUGAVPILS001","LIEPAJA001","JELGAVA001",
            "VILNIUS001","KAUNAS001","KLAIPEDA001",
            "TALLINN001","TARTU001","NARVA001",
            "MADRID001","ROME001","AMSTERDAM001",
            "BRUSSELS001","STOCKHOLM001","HELSINKI001"
        ]
    },
    {
        "sensor_id": "SENS-004",
        "sensor_name": "wind_degree",
        "metric": "wind_direction",
        "unit": "°",
        "device_id": [
            "RIGA001","PARIS001","BERLIN001","LONDON001",
            "DAUGAVPILS001","LIEPAJA001","JELGAVA001",
            "VILNIUS001","KAUNAS001","KLAIPEDA001",
            "TALLINN001","TARTU001","NARVA001",
            "MADRID001","ROME001","AMSTERDAM001",
            "BRUSSELS001","STOCKHOLM001","HELSINKI001"
        ]
    },
    {
        "sensor_id": "SENS-005",
        "sensor_name": "humidity",
        "metric": "relative_humidity",
        "unit": "%",
        "device_id": [
            "RIGA001","PARIS001","BERLIN001","LONDON001",
            "DAUGAVPILS001","LIEPAJA001","JELGAVA001",
            "VILNIUS001","KAUNAS001","KLAIPEDA001",
            "TALLINN001","TARTU001","NARVA001",
            "MADRID001","ROME001","AMSTERDAM001",
            "BRUSSELS001","STOCKHOLM001","HELSINKI001"
        ]
    },
    {
        "sensor_id": "SENS-6",
        "sensor_name": "pressure_mb",
        "metric": "air_pressure",
        "unit": "mb",
        "device_id": [
            "RIGA001","PARIS001","BERLIN001","LONDON001",
            "DAUGAVPILS001","LIEPAJA001","JELGAVA001",
            "VILNIUS001","KAUNAS001","KLAIPEDA001",
            "TALLINN001","TARTU001","NARVA001",
            "MADRID001","ROME001","AMSTERDAM001",
            "BRUSSELS001","STOCKHOLM001","HELSINKI001"
        ]
    }
]
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
    

MDP_DADABASE = os.getenv("MDP_DADABASE")

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
    cursor.close()
    conn.close()


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

# Insert devices first so sensors' foreign key to devices is satisfied
for ville in list_ville:
    insert_data_device(ville, ville["lat"], ville["lon"])

# Then insert sensors (devices now exist)
for sensor in sensor_id:
    for device_id in sensor["device_id"]:
        row = sensor.copy()
        row["device_id"] = device_id
        print("Inserting sensor:", row)
        # lat/lon not needed for sensor insertion; pass None
        insert_data_sensors(row, None, None)