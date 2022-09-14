import pandas as pd
import boto3
from datetime import datetime, timedelta
import os

def main():

    filepath_streaming = download_from_s3("streaming-velib")
    df_streaming = pd.read_json(filepath_streaming)
    df_streaming = pd.json_normalize(df_streaming["data"]["stations"])
    df_streaming["num_bikes_available_types"] = df_streaming["num_bikes_available_types"].apply(flatten)
    df_streaming[['ebike_available','mechanical_available']] = df_streaming.num_bikes_available_types.str.split(" ",expand=True,)


    filepath_distance = download_from_s3("raw-file-distance")
    df_distance = pd.read_parquet(filepath_distance)
    df_prediction_rain = will_it_rain(df_distance)

    from extract_gmap import upload_s3
    
    upload_s3(df_streaming, "processed-file-streaming", "processed-df-streaming")
    upload_s3(df_prediction_rain, "processed-file-weather", "processed-df-weather")
    upload_s3(df_distance, "processed-file-distance", "processed-df-distance")



def download_from_s3(content):
    s3 = boto3.client('s3',
        aws_access_key_id = os.getenv("AWS_ACCESS_KEY"),
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        )
    my_bucket = 'velib-streaming-weather'
    list_files = []

    if content == "streaming-velib":
        response = s3.list_objects_v2(
            Bucket=my_bucket,
            Prefix='streaming-velib/')
    else:
        response = s3.list_objects_v2(
            Bucket=my_bucket,
            Prefix=f'batch-velib/{content}/')
            
    for file in response.get('Contents', []):
        list_files.append(file['Key'])
    
    date_last_file = get_last_file(content, list_files)

    for file in list_files:
        if date_last_file:
            if date_last_file in file:
                return download_file(s3, my_bucket, file)
    


def get_last_file(content, list_files):
    list_files_dates = []
    for file in range(1, len(list_files)):
        if content == "streaming-velib":
            list_name = list_files[file].split(".")[0].split('-')
            list_name = "-".join(list_name[-6:])
        else:
            list_name = list_files[file].split('-')
            list_name = "-".join(list_name[-6:])
        list_name_date = datetime.strptime(list_name, "%m-%d-%Y-%H-%M-%S")
        list_files_dates.append(list_name_date)
    df = pd.DataFrame(list_files_dates)

    if len(df) > 0:
        maximum = df[0].max()
        return maximum.strftime('%m-%d-%Y-%H-%M-%S')
    else:
        return None

def download_file(s3, my_bucket, file):
    name_file = file.split("/")[-1]
    s3.download_file(my_bucket, file, f"./{name_file}")
    return f"./{name_file}"

def will_it_rain(df_distance):
    departure_station = df_distance.iloc[:, 0]
    distance_time = df_distance.loc[:, df_distance.columns != "departure_station"].applymap(rain)
    return pd.concat([departure_station, distance_time], axis=1)
    
def rain(time):
    filepath_weather = download_from_s3("raw-file-weather")
    df_weather = pd.read_parquet(filepath_weather)

    df_arriving = weather_before_arrive(time,df_weather)
    df_arriving = df_arriving.reset_index()
    print(df_arriving)
    return prediction_rain(df_arriving)

def weather_before_arrive(time, df_weather):

    time_ride = datetime.strptime(time, "%H:%M:%S")
    minutes = time_ride.minute + time_ride.hour * 60
    now = datetime.now()
    arriving_time = now + timedelta(minutes=minutes)
    arriving_time = arriving_time.strftime("%Y-%m-%d %H:%M:%S")
    df_weather['dt'] = pd.to_datetime(df_weather['dt'])
    return df_weather[~(df_weather['dt'] > arriving_time)]

def prediction_rain(df):
    if len(df) > 0:
        if df["precipitation"].max() > 8 :
            return "Heavy"
        elif df["precipitation"].max() > 4:
            return "Moderate"
        elif df["precipitation"].max() > 0:
            return "Low"
        elif df["precipitation"].max() == 0:
            return "No rain"

def flatten(dict):
    somme_ebike = 0
    somme_mech = 0
    for bike in dict:
        ebike = bike.get("ebike")
        if ebike is None:
            ebike = 0
        somme_ebike += int(ebike)
        mechanical = bike.get("mechanical")
        if mechanical is None:
            mechanical = 0
        somme_mech += int(mechanical)
    string = f"{somme_ebike} {somme_mech}"
    return string

if __name__ == "__main__":
    main()