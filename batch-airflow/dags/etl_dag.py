import logging
from os import environ
from json import load
from datetime import datetime, date, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operator.dummyoperator import DummyOperator

with DAG(
    dag_id = "gmap_distance",
    schedule_interval = "0 23 * * 2",  # Every week
    catchup = True,
    max_active_runs = 3,
    start_date = datetime(2022, 9, 9, 20)
    default_args = {
        "owner": "airflow",
        "depends_on_past": True,
        "retries": 0
    }
) as gmap_distance:

    start_extract_gmap = DummyOperator(
        task_id = "start_extract_gmap"
    )

    extract_gmap = BashOperator(
        task_id = "extract_gmap"
        bash_command = "python etl_layers/extract_gmap.py"
    )

    process_gmap = BashOperator(
        task_id = "process_gmap"
        bash_command = "etl_layers/raw_files_processing.py"
    )

    upload_gmap = BashOperator(
        task_id = "upload_gmap"
        bash_command = "etl_layers/processed_files_uploading.py"
    )

    end_extract_gmap = DummyOperator(
        task_id = "end_extract_gmap"
    )

start_extract_gmap > extract_gmap
extract_gmap > process_gmap
process_gmap > upload_gmap
upload_gmap > end_extract_gmap

with DAG(
    dag_id = "weather",
    schedule_interval = "0 23 * * 2",  # Every week
    catchup = True,
    max_active_runs = 3,
    start_date = datetime(2022, 9, 9, 20)
    default_args = {
        "owner": "airflow",
        "depends_on_past": True,
        "retries": 0
    }
) as weather:

    start_extract_weather = DummyOperator(
        task_id = "start_extract_weather"
    )

    extract_weather = BashOperator(
        task_id = "extract_weather"
        bash_command = "python etl_layers/extract_weather.py"
    )

    process_weather = BashOperator(
        task_id = "process_weather"
        bash_command = "etl_layers/raw_files_processing.py"
    )

    upload_weather = BashOperator(
        task_id = "upload_weather"
        bash_command = "etl_layers/processed_files_uploading.py"
    )

    end_extract_weather = DummyOperator(
        task_id = "end_extract_weather"
    )

start_extract_weather > extract_weather
extract_weather > process_weather
process_weather > upload_weather
upload_weather > end_extract_weather

with DAG(
    dag_id = "weather",
    schedule_interval = "0 23 * * 2",  # Every week
    catchup = True,
    max_active_runs = 3,
    start_date = datetime(2022, 9, 9, 20)
    default_args = {
        "owner": "airflow",
        "depends_on_past": True,
        "retries": 0
    }
) as weather:

    start_extract_weather = DummyOperator(
        task_id = "start_extract_weather"
    )

    extract_weather = BashOperator(
        task_id = "extract_weather"
        bash_command = "python etl_layers/extract_weather.py"
    )

    process_weather = BashOperator(
        task_id = "process_weather"
        bash_command = "etl_layers/raw_files_processing.py"
    )

    upload_weather = BashOperator(
        task_id = "upload_weather"
        bash_command = "etl_layers/processed_files_uploading.py"
    )

    end_extract_weather = DummyOperator(
        task_id = "end_extract_weather"
    )

start_extract_weather > extract_weather
extract_weather > process_weather
process_weather > upload_weather
upload_weather > end_extract_weather

with DAG(
    dag_id = "streaming",
    schedule_interval = "0 23 * * 2",  # Every week
    catchup = True,
    max_active_runs = 3,
    start_date = datetime(2022, 9, 9, 20)
    default_args = {
        "owner": "airflow",
        "depends_on_past": True,
        "retries": 0
    }
) as streaming:

    start_streaming = DummyOperator(
        task_id = "start_extract_streaming"
    )

    process_streaming = BashOperator(
        task_id = "process_streaming"
        bash_command = "etl_layers/raw_files_processing.py"
    )

    upload_streaming = BashOperator(
        task_id = "upload_streaming"
        bash_command = "etl_layers/processed_files_uploading.py"
    )

    end_extract_streaming = DummyOperator(
        task_id = "end_extract_streaming"
    )

start_streaming > process_streaming
process_streaming > upload_streaming
upload_streaming > end_extract_streaming
