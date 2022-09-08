import pandas as pd
import requests
import logging as log
import json
import boto3
import fastparquet
from datetime import datetime

def execute():
    raw_json =  fetch_API("station_information")
    raw_stations = raw_json["data"]["stations"]
    raw_df = pd.DataFrame(raw_stations)

    upload_s3(raw_df, "raw_df", "raw_df_velib")
    
    raw_distance = pd.DataFrame()
    while raw_stations:
        print(raw_stations[0])
        lat_dep = raw_stations[0]["lat"]
        lon_dep = raw_stations[0]["lon"]
        lat_dest = raw_stations[1]["lat"]
        lon_dest = raw_stations[1]["lon"]
        fetch_API("gmap_distance", lat_dep, lon_dep, lat_dest, lon_dest)

    # et vice versa au cas où les distances ne sont pas les mêmes


        





def fetch_API(content, lat_dep = None, lon_dep = None, lat_dest = None, lon_dest = None):
    try:
        if content == "station_information":
            velib_url = "https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/station_information.json"
            r =  requests.get(velib_url)
            return r.json()
        if content == "gmap_distance":
            API_KEY_GOOGLE = ""
            gmap_url = f"https://maps.googleapis.com/maps/api/distancematrix/json?origins={lat_dep}%2C{lon_dep}&destinations={lat_dest}%2C{lon_dest}&mode=bicycling&key={API_KEY_GOOGLE}"
            r = requests.get(gmap_url)
        raise

    except Exception as e:
        log.info(f"Could't call the API Reason: {e}")

def upload_s3(df, destination, name):

    filename = convert_parquet(df, name)

    s3 = boto3.resource('s3',
        aws_access_key_id = "",
        aws_secret_access_key = ""
        )

    if destination == "raw_df":
        object_raw = s3.Object('batch-velib', "raw-file-velib")
        object_raw.put(Body = filename)


def convert_parquet(df, name):
    date = datetime.utcnow().strftime("%m-%d-%Y-%H-%M-%S")
    return df.to_parquet(f"name{date}")


execute()
