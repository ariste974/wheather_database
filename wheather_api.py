import os
import sys
import requests
from pprint import pprint

def get_weatherapi_current(api_key, city):
    url = "http://api.weatherapi.com/v1/current.json"
    params = {"key": api_key, "q": city}

    data = requests.get(url, params=params).json()

    return {
        "wind_kph": data["current"]["wind_kph"],
        "wind_degree": data["current"]["wind_degree"],
        "wind_dir": data["current"]["wind_dir"]
    }