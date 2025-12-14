import psycopg2

from dotenv import load_dotenv
import os

load_dotenv()
MDP_DADABASE = os.getenv("MDP_DADABASE")

def insert_data(row):
    conn = psycopg2.connect(
        host="16.170.202.253",
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
        sunshine_duration_s,
        prcp,
        wind_kph,
        wind_degree,
        wind_dir
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s);
    """

    cursor.execute(query, (
        row["timestamp_utc"],                           # datetime → OK
        float(row["temperature_2m"]) if row["temperature_2m"] is not None else None,
        float(row["sunshine_duration_s"]) if row["sunshine_duration_s"] is not None else None,
        float(row["prcp"]) if row["prcp"] is not None else None,
        float(row["wind_kph"]) if row["wind_kph"] is not None else None,
        int(row["wind_degree"]) if row["wind_degree"] is not None else None,
        str(row["wind_dir"]) if row["wind_dir"] is not None else None
    ))

    conn.commit()
    cursor.close()
    conn.close()
