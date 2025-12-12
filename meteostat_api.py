# Import Meteostat library and dependencies
from datetime import datetime
from meteostat import Hourly
from datetime import datetime, timedelta
import pandas as pd

def get_meteostat_current(station):
    now = datetime.now()
    start = now - timedelta(hours=1)

    data = Hourly(station, start, now).fetch()

    if "prcp" not in data.columns or data.empty:
        return {"prcp": None}

    value = data["prcp"].iloc[-1]

    # Si NA → None
    if pd.isna(value):
        return {"prcp": None}

    # Sinon converti en float
    return {"prcp": float(value)}