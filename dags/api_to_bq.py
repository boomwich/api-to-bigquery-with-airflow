import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

import pandas as pd
import requests

from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# For PythonOperator

API_KEY = os.getenv("WEATHER_API_KEY")
json_api_result_path = "/home/airflow/data/json_result_from_api.csv"
weather_df_path = "/home/airflow/data/weather.csv"
air_quality_df_path = "/home/airflow/data/air_quality.csv"
GCS_bucket_name = "bucket-to-bigquery"

def get_data_from_api():
    url = "https://api.weatherapi.com/v1/current.json?key=" + API_KEY + "&q=Bangkok&aqi=yes"
    response = requests.get(url)
    json_result = response.json()
    json_normalized_df = pd.json_normalize(json_result)
    json_normalized_df.to_csv(json_api_result_path)

def split_data_with_pandas():
    json_df = pd.read_csv("/home/airflow/data/json_result_from_api.csv")
    json_df.columns = [col.replace('location.', '').replace('current.','') for col in json_df.columns]

    weather_column = ['name', 'country', 'last_updated', 'temp_c', 'feelslike_c', 'condition.text', 'humidity']
    weather_df = json_df[weather_column]
    weather_df.to_csv(weather_df_path, index=False)

    air_quality_df = pd.concat([json_df[['name', 'country', 'last_updated']], json_df.filter(regex='^(air_quality).*$')], axis=1)
    air_quality_df.to_csv(air_quality_df_path, index=False)

# Default Args

default_args = {
    'owner': 'Weather',
    'depends_on_past': False,
    'catchup': False,
    'start_date': days_ago(0),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Create DAG

dag = DAG(
    'Collect_weather_info',
    default_args=default_args,
    description='Simple pipeline for ETL weather and air quality data',
    schedule_interval='2/15 * * * *',
)

# Tasks

t1 = PythonOperator(
    task_id='api_call',
    python_callable=get_data_from_api,
    dag=dag,
)

t2 = PythonOperator(
    task_id='split_data',
    python_callable=split_data_with_pandas,
    dag=dag,
)

t3 = LocalFilesystemToGCSOperator(
    task_id='local_file_to_gcs',
    src=[weather_df_path, air_quality_df_path],
    dst='',
    bucket=GCS_bucket_name,
)

t4 = GCSToBigQueryOperator(
    task_id='weather_gcs_to_bq',
    bucket=GCS_bucket_name,
    source_objects="weather.csv",
    destination_project_dataset_table='weather_and_air_quality.weather',
    write_disposition="WRITE_APPEND",
)

t5 = GCSToBigQueryOperator(
    task_id='air_quality_gcs_to_bq',
    bucket=GCS_bucket_name,
    source_objects="air_quality.csv",
    destination_project_dataset_table='weather_and_air_quality.air_quality',
    write_disposition="WRITE_APPEND",
)

# Dependencies

t1 >> t2 >> t3 >> [t4,t5]