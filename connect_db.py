import psycopg2

from dotenv import load_dotenv
import os
import pandas as pd

import pandas as pd
import math


load_dotenv()
MDP_DADABASE = os.getenv("MDP_DADABASE")
host="51.20.31.180"

def safe_int(value):
    if value is None or pd.isna(value):
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def safe_float(value):
    if value is None or pd.isna(value):
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def safe_str(value):
    if value is None or pd.isna(value):
        return None
    return str(value)


def safe_datetime(value):
    if value is None or pd.isna(value):
        return None
    return value  # déjà datetime tz-aware dans ton cas

def insert_data(row, lat, lon):
    conn = psycopg2.connect(
        host=host,
        port=5432,
        dbname="weatherdb",
        user="weatheruser",
        password=MDP_DADABASE
    )

    cursor = conn.cursor()


    query_weather = """INSERT INTO weather_readings ( device_id, reading_ts, temperature_2m,prcp,wind_kph,wind_degree,humidity,pressure_mb) VALUES (%s, %s, %s, %s, %s, %s, %s, %s); """

    cursor.execute(query_weather, (
    safe_str(row.get("device_id")),
    safe_datetime(row.get("time")),
    safe_float(row.get("temperature_2m")),
    safe_float(row.get("prcp")),
    safe_float(row.get("wind_kph")),
    safe_int(row.get("wind_degree")),
    safe_float(row.get("humidity")),
    safe_float(row.get("pressure_mb"))
))



    conn.commit()
    cursor.close()
    conn.close()

