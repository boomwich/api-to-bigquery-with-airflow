# api-to-bigquery-with-airflow

A simple pipeline that pulls data from API from [Weather API](https://www.weatherapi.com/), separate the data, upload them to Google Cloud Storage, and store them in Google BigQuery.

Airflow is setup by using docker image from [DockerHub](https://hub.docker.com/r/apache/airflow) and following the [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html) page.