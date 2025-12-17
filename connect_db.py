import psycopg2

from dotenv import load_dotenv
import os
import pandas as pd
load_dotenv()
MDP_DADABASE = os.getenv("MDP_DADABASE")

def insert_data(row, lat, lon):
    conn = psycopg2.connect(
        host="16.171.133.144",
        port=5432,
        dbname="weatherdb",
        user="weatheruser",
        password=MDP_DADABASE
    )

    cursor = conn.cursor()

    query = """
INSERT INTO meteo (
    time,
    temperature_2m,
    prcp,
    wind_kph,
    wind_degree,
    humidity,
    pressure_mb,
    location
)
VALUES (
    %s, %s, %s, %s, %s, %s, %s,
    ST_SetSRID(ST_MakePoint(%s, %s), 4326)
);
"""

    cursor.execute(query, (
    row["time"],                                         # datetime tz-aware
    float(row["temperature_2m"]) if row["temperature_2m"] is not None else None,
    float(row["prcp"]) if row["prcp"] is not None else None,
    float(row["wind_kph"]) if row["wind_kph"] is not None else None,
    int(row["wind_degree"]) if row["wind_degree"] is not None else None,
    float(row.get("humidity")) if row.get("humidity") is not None else None,
    float(row.get("pressure_mb")) if row.get("pressure_mb") is not None else None,
    float(lon),                                         # longitude
    float(lat)                                          # latitude
))


    conn.commit()
    cursor.close()
    conn.close()
