import sys
sys.path.insert(0, "/opt/airflow/ingestion")

from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator

from airflow import DAG

from ingestion.tfl_client import TFLClient


def ingest_line_status(execution_date, **context):
    client = TFLClient()
    data = client.fetch_line_status()
    key = client.upload_to_bronze(data, "line_status", execution_date)
    context["ti"].xcom_push(key="s3_key", value=key)
    return f"Uploaded {len(data)} line status records"


def ingest_disruptions(execution_date, **context):
    client = TFLClient()
    data = client.fetch_disruptions()
    key = client.upload_to_bronze(data, "disruptions", execution_date)
    context["ti"].xcom_push(key="s3_key", value=key)
    return f"Uploaded {len(data)} disruption records"


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="tfl_ingestion",
    description="Hourly ingestion of TfL tube status and disruptions to S3 Bronze",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "tfl", "bronze"],
) as dag:

    line_status_task = PythonOperator(
        task_id="ingest_line_status",
        python_callable=ingest_line_status,
    )

    disruptions_task = PythonOperator(
        task_id="ingest_disruptions",
        python_callable=ingest_disruptions,
    )

    # Run both ingestions in parallel
    [line_status_task, disruptions_task]  # noqa: B018
