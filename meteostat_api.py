# Import Meteostat library and dependencies
from datetime import datetime
from meteostat import Hourly
from datetime import datetime, timedelta
import pandas as pd


def get_meteostat_current(station):
    end = datetime.utcnow()
    start = end - timedelta(hours=1)

    df = Hourly(station, start, end).fetch()

    # Cas vide ou colonne absente
    if df.empty or "prcp" not in df.columns:
        return pd.DataFrame([{
            "time": end,
            "prcp": None
        }])

    # Récupération de la dernière valeur horaire
    value = df["prcp"].iloc[-1]

    if pd.isna(value):
        value = None
    else:
        value = float(value)

    return pd.DataFrame([{
        "time": df.index[-1],  # heure réelle de mesure
        "prcp": value
    }])


def get_meteostat_last_24h(station):
    end = datetime.utcnow()
    start = end - timedelta(hours=24)

    df = Hourly(station, start, end).fetch()

    if df.empty or "prcp" not in df.columns:
        return pd.DataFrame(columns=["time", "prcp"])

    df = df[["prcp"]].reset_index()
    df["prcp"] = df["prcp"].astype(float)

    return df