from datetime import datetime, timedelta

from airflow.providers.apache.spark.operators.spark_submit import (  # type: ignore
    SparkSubmitOperator,  # type: ignore
)
from airflow.sensors.external_task import ExternalTaskSensor  # type: ignore

from airflow import DAG

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

SPARK_CONF = {
    "spark.hadoop.fs.s3a.endpoint": "http://localstack:4566",
    "spark.hadoop.fs.s3a.access.key": "test",
    "spark.hadoop.fs.s3a.secret.key": "test",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
}

with DAG(
    dag_id="spark_bronze_to_silver",
    description="Transforms Bronze JSON to Silver Parquet using Spark",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    tags=["spark", "silver", "transform"],
) as dag:
    # Wait for both ingestion DAGs to complete for this hour
    wait_for_tfl = ExternalTaskSensor(
        task_id="wait_for_tfl_ingestion",
        external_dag_id="tfl_ingestion",
        external_task_id=None,  # waits for the whole DAG run
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    wait_for_weather = ExternalTaskSensor(
        task_id="wait_for_weather_ingestion",
        external_dag_id="weather_ingestion",
        external_task_id=None,
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    transform_tfl = SparkSubmitOperator(
        task_id="transform_tfl_bronze_to_silver",
        application="/opt/spark/jobs/bronze_to_silver_tfl.py",
        conn_id="spark_default",
        conf=SPARK_CONF,
        application_args=["{{ ds }}"],  # passes execution date as argument
    )

    transform_weather = SparkSubmitOperator(
        task_id="transform_weather_bronze_to_silver",
        application="/opt/spark/jobs/bronze_to_silver_weather.py",
        conn_id="spark_default",
        conf=SPARK_CONF,
        application_args=["{{ ds }}"],
    )

    [wait_for_tfl, wait_for_weather] >> transform_tfl
    [wait_for_tfl, wait_for_weather] >> transform_weather
