from datetime import datetime
import pandas as pd
from extract_gmap import fetch_API, upload_s3
from datetime import datetime

def main():
    raw_weather = fetch_API("weather")
    raw_weather_minutely = raw_weather.get("minutely")
    df_weather = retrieve_precipitation(raw_weather_minutely)
    upload_s3(df_weather, "raw-file-weather", "raw-df-weather")

def retrieve_precipitation(weather):
    df = pd.DataFrame(weather)
    df["dt"] = [datetime.fromtimestamp(x) for x in df["dt"]]
    return df

if __name__ == "__main__":
    main()