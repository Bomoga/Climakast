import requests
import json
import pandas as pd
import time

BASEURL = "https://power.larc.nasa.gov/api/temporal/monthly/point"

PARAMS = {
    "parameters": "T2M,PRECTOT,WS50M,ALLSKY_SFC_SW_DWN",      # 2-meter temp, total precip, wind at 50m (turbines), insolation
    "community": "AG",
    "start": 2000,                    
    "end": 2020,                      
    "format": "JSON"
}

global_df = pd.read_csv("data/external/global-data-on-sustainable-energy.csv")
list_of_countries = set(global_df['country'].unique())

countries_df = pd.read_csv("data/external/countries.csv")
countries = countries_df.dropna(subset=["latitude", "longitude"])
countries = countries.drop_duplicates(subset=["country"])
countries = countries[countries["name"].isin(list_of_countries)]

country_coords = list(countries[["country", "latitude", "longitude"]].itertuples(index=False, name=None))

def fetch_api_data(name, lon, lat, start=2000, end=2020, session=None, pause=0.5):
    s = session or requests.Session()
    parameters = {**PARAMS, **{"longitude": lon, "latitude": lat, "start": start, "end": end}}
    try:
        r = s.get(BASEURL, params=parameters, timeout=30)
        r.raise_for_status()
        data = r.json()

        filename = f"data/raw/nasa_power/{name}_{start}-{end}_T2M-PRECTOT-WS50M-ALLSKY_SFC_SW_DWN.json"

        with open(filename, "w") as f:
            json.dump(data, f, indent=2)

        return filename
    
    except Exception as e:
        print(f"Error fetching data for {name}: {e}")
        return None
    
    finally:
        time.sleep(pause)

for country in country_coords:
    name = country[0]
    lat = country[1]
    lon = country[2]
    fetch_api_data(name, lon, lat)