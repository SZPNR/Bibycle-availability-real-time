import pandas as pd
import boto3
from sqlalchemy import create_engine
from sqlalchemy.dialects import registry
from datetime import datetime
import os

def main():

    filepath_distance = download_from_s3("processed-file-distance")
    filepath_streaming = download_from_s3("processed-file-streaming")
    filepath_weather = download_from_s3("processed-file-weather")

    df_distance = pd.read_parquet(filepath_distance)
    df_streaming = pd.read_parquet(filepath_streaming)
    df_weather = pd.read_parquet(filepath_weather)

    snowflake_username = os.getenv("SNOW_USER")
    snowflake_password = os.getenv("SNOW_PWD")
    snowflake_account = os.getenv("SNOW_ACCOUNT")
    snowflake_warehouse = "velib"
    snowflake_database = 'VELIB'
    snowflake_schema = 'public'


    registry.register('snowflake', 'snowflake.sqlalchemy', 'dialect')
    engine = create_engine(
        'snowflake://{user}:{password}@{account}/{db}/{schema}?warehouse={warehouse}'.format(
            user=snowflake_username,
            password=snowflake_password,
            account=snowflake_account,
            db=snowflake_database,
            schema=snowflake_schema,
            warehouse=snowflake_warehouse,
        )
    )
    

    df_distance.to_sql('distance', con=engine, schema='public', index=False, if_exists='replace')
    df_streaming.to_sql('streaming', con=engine, schema='public', index=False, if_exists='replace')
    df_weather.to_sql('weather', con=engine, schema='public', index=False, if_exists='replace')

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
    for file in range(len(list_files)):
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

if __name__ == "__main__":
    main()