# Bibycle-availability-real-time

# Chess to S3 to Snowflake Project

`Data pipeline collecting data from multiple APIs to get access to real-time availability of public-use Velib bikes in Paris in addition to prediction of time journey and the weather.`


## General information

The data pipeline follows a lambda architecture with one streaming source and two batch sources. The first one fetch the data from official Velib API to get the availability of different bikes and available empty spaces for parking. The data is then transfered from Kafka to Spark on Scala and finally sent to an S3 folder.
The latter get the data from the Google Maps and OpenWeather APIs to extract the weather of the next hour and the time journey between each station.

All the raw data is processed with Python to have the time for each journey and the weather for each journey. 

The data is finally sent to Snowflake as relational table.

Each step is containerized in Docker and scheduled by Apache Airfow. The containerization is divided into two networks, the first on for batch pipeline and the second for streaming pipeline.


## Visual explanation!

Fig1. Visual explanation of how the data pipeline works
[Présentation2](https://user-images.githubusercontent.com/94069984/191972291-41d35542-8dc6-49a7-9296-ce8a7a3e181c.jpg)


## Files

### batch-airflow

All the process from the extraction from Google Maps and Openweather to sending the processed data to Snowflake.

### streaming_kafka

All the process from the extraction from the Velib API to sending the raw data to AWS S3.

### AWS_S3

Pictures of the different files in the bucket

### Snowflake

Pictures of the different tables on Snowflake

## Opportunities for improvement

* Implement Kubernetes to improve scalability for multiples Docker containers
* Put the project on an EC2 instance so it can run all the time
* Create a website or an application so it can have real users
