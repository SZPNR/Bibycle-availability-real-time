import pandas as pd
import requests
import logging as log
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import time
import os

def main():
    raw_json =  fetch_API("station_information")
    raw_stations = raw_json["data"]["stations"]
    raw_df = pd.DataFrame(raw_stations)
    df_distance = pd.DataFrame()

    upload_s3(raw_df, "raw-file-velib", "raw-df-velib")

    for station in range(len(raw_stations)):
        raw_distance = pd.DataFrame()
        window_iteration = 1
        raw_stations_windows = raw_stations[1:24]
        while raw_stations_windows:
            windowed_destinations_coords = extract_destinations_coord(raw_stations_windows) #liste de 24
            lat_dep = raw_stations[station]["lat"]
            lon_dep = raw_stations[station]["lon"]
            response_json = fetch_API("gmap_distance", lat_dep, lon_dep, windowed_destinations_coords) #reponse de 24 par rapport Paris
            while response_json["status"] == "UNKNOWN_ERROR":
                time.sleep(2)
                response_json = fetch_API("gmap_distance", lat_dep, lon_dep, windowed_destinations_coords)

            new_row = add_row_time_df(response_json, raw_stations[station], raw_stations_windows)
            raw_distance = pd.concat([raw_distance, new_row], ignore_index=True)
            window_iteration += 24
            raw_stations_windows = raw_stations[window_iteration:24+window_iteration]

        raw_distance = pd.concat([df1.apply(lambda x: sorted(x, key=pd.isnull)) for _, df1 in raw_distance.groupby("departure_station")]).dropna()

        df_distance = pd.concat([df_distance, raw_distance], ignore_index=True)

    upload_s3(df_distance, "raw-file-distance", "raw-df-distance")


def fetch_API(content, lat_dep = None, lon_dep = None, desination_coords = None):
    try:
        if content == "station_information":
            velib_url = "https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/station_information.json"
            r =  requests.get(velib_url)
            return r.json()
        if content == "gmap_distance":
            API_KEY_GOOGLE = os.getenv("API_KEY_GOOGLE")
            gmap_url = generate_gmap_link(lat_dep, lon_dep, desination_coords, API_KEY_GOOGLE) #url de 24 destinations
            r = requests.get(gmap_url)
            print(r.json())
            return r.json()
        if content == "weather":
            openweather_key = os.getenv("API_KEY_WEATHER")
            weather_url = f"https://api.openweathermap.org/data/3.0/onecall?lat=48.52&lon=2.19&exclude=hourly,daily&appid={openweather_key}"
            r = requests.get(weather_url)
            return r.json()

    except Exception as e:
        log.info(f"Could't call the API Reason: {e}")

def extract_destinations_coord(stations):

    all_stations = []
    for destination in stations:
        lat_dest = destination["lat"]
        lon_dest = destination["lon"]
        coords_dict = {"lat": lat_dest, "lon": lon_dest}
        all_stations.append(coords_dict)
    return all_stations

def generate_gmap_link(lat_dep, lon_dep, destination_coords, API_KEY_GOOGLE):
    url = f"https://maps.googleapis.com/maps/api/distancematrix/json?origins={lat_dep}%2C{lon_dep}&destinations="
    str_coords = ""
    for station in range(len(destination_coords) - 1):
        str_coords += f"{destination_coords[station].get('lat')}%2C{destination_coords[station].get('lon')}%7C"
    str_coords += f"{destination_coords[-1].get('lat')}%2C{destination_coords[-1].get('lon')}"

    return f"{url}{str_coords}&mode=bicycling&key={API_KEY_GOOGLE}"

def retrieve_time_distance(distance_json):
    duration = []
    for time in distance_json["rows"][0]["elements"]:
        time_distance = time["duration"]["text"]
        if "hour" in time_distance:
            if "mins" in time_distance:
                if "hours" in time_distance:
                    time_value = datetime.strptime(time_distance, "%H hours %M mins")
                else:
                    time_value = datetime.strptime(time_distance, "%H hour %M mins")
            else:
                if "hours" in time_distance:
                    time_value = datetime.strptime(time_distance, "%H hours %M min")
                else:
                    time_value = datetime.strptime(time_distance, "%H hour %M min")
        elif "min" in time_distance:
            if "mins" in time_distance:
                time_value = datetime.strptime(time_distance, "%M mins")
            else:
                time_value = datetime.strptime(time_distance, "%M min")
        normalized_time_value = datetime.strftime(time_value,"%H:%M:%S")
        duration.append(normalized_time_value)
    return duration

def add_row_time_df(raw_distance_json, departure, windowed_destination):

    time_df = pd.DataFrame()
    time_list = retrieve_time_distance(raw_distance_json)

    departure["name"] = departure["name"].replace(" - ","-")
    departure["name"] = departure["name"].replace("   ","-")
    departure["name"] = departure["name"].replace("  ","-")
    departure["name"] = departure["name"].replace(" ","-")
    departure["name"] = departure["name"].replace("'","-")
    departure["name"] = departure["name"].replace("é","e")
    departure["name"] = departure["name"].replace("è","e")
    departure["name"] = departure["name"].replace("ê","e")
    departure["name"] = departure["name"].replace("ë","e")
    departure["name"] = departure["name"].replace("à","a")
    departure["name"] = departure["name"].replace("â","a")
    departure["name"] = departure["name"].replace("ç","c")
    departure["name"] = departure["name"].replace("'","-")

    for station in range(len(windowed_destination)):
        windowed_destination[station]["name"] = windowed_destination[station]["name"].replace(" - ","-")
        windowed_destination[station]["name"] = windowed_destination[station]["name"].replace("   ","-")
        windowed_destination[station]["name"] = windowed_destination[station]["name"].replace("  ","-")
        windowed_destination[station]["name"] = windowed_destination[station]["name"].replace(" ","-")
        windowed_destination[station]["name"] = windowed_destination[station]["name"].replace("'","-")
        windowed_destination[station]["name"] = windowed_destination[station]["name"].replace("é","e")
        windowed_destination[station]["name"] = windowed_destination[station]["name"].replace("è","e")
        windowed_destination[station]["name"] = windowed_destination[station]["name"].replace("ê","e")
        windowed_destination[station]["name"] = windowed_destination[station]["name"].replace("ë","e")
        windowed_destination[station]["name"] = windowed_destination[station]["name"].replace("à","a")
        windowed_destination[station]["name"] = windowed_destination[station]["name"].replace("â","a")
        windowed_destination[station]["name"] = windowed_destination[station]["name"].replace("ç","c")
        windowed_destination[station]["name"] = windowed_destination[station]["name"].replace("'","-")
        
        new_row = {"departure_station": departure["name"], windowed_destination[station]["name"]: time_list[station]}
        time_df_row = pd.DataFrame(data=new_row, columns= ["departure_station", windowed_destination[station]["name"]], index=[0])
        time_df =  pd.concat([time_df, time_df_row])

    time_df = pd.concat([df1.apply(lambda x: sorted(x, key=pd.isnull)) for _, df1 in time_df.groupby("departure_station")]).dropna()
    return time_df

def upload_s3(df, destination, name):

    filepath = convert_parquet(df, name)

    s3 = boto3.client('s3',
        aws_access_key_id = os.getenv("AWS_ACCESS_KEY"),
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        )

    s3.upload_file(filepath, 'velib-streaming-weather', f"batch-velib/{destination}/{filepath}")


def convert_parquet(df, name):
    date = datetime.now().strftime("%m-%d-%Y-%H-%M-%S")
    filepath = f"{name}-{date}"

    parquet_table = pa.Table.from_pandas(df)
    pq.write_table(parquet_table, filepath)

    return filepath

if __name__ == "__main__":
    main()