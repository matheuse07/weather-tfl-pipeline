import sys
sys.path.insert(0, "/opt/airflow/ingestion")

from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator

from airflow import DAG
from ingestion.weather_client import WeatherClient


def ingest_weather_forecast(execution_date, **context):
    client = WeatherClient()
    data = client.fetch_forecast()
    key = client.upload_to_bronze(data, execution_date)
    context["ti"].xcom_push(key="s3_key", value=key)
    return "Weather forecast uploaded"


default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="weather_ingestion",
    description="Hourly ingestion of Open-Meteo weather data to S3 Bronze",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "weather", "bronze"],
) as dag:
    weather_forecast_task = PythonOperator(
        task_id="ingest_weather_forecast",
        python_callable=ingest_weather_forecast,
    )
