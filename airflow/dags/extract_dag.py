from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import logging
import requests

log = logging.getLogger("airflow.task")

from api_ingestion.api_ingestion import (
    ingest_all_pages,
    load_watermark,
    max_timestamp,
    save_watermark,
    upload_to_minio,
)

SPARK_JOBS_PATH = "/opt/spark/jobs"


def ingest_tms_api(data_date=None):
    wm = load_watermark()
    if wm is None and os.getenv("TRACKING_INITIAL_SINCE"):
        wm = os.getenv("TRACKING_INITIAL_SINCE")

    events = ingest_all_pages(since=wm)

    if not events:
        return

    upload_to_minio(events, data_date=data_date)
    m = max_timestamp(events)
    if m:
        save_watermark(m)

def should_run_api() -> bool:
    wm = load_watermark()
    if wm is None and os.getenv("TRACKING_INITIAL_SINCE"):
        wm = os.getenv("TRACKING_INITIAL_SINCE")
    params = {"page": 1, "limit": 1}
    if wm is not None:
        params["since"] = wm
    r = requests.get(
        f"{os.getenv('TMS_API_BASE_URL', 'http://tms-api:8085')}/api/v1/tracking",
        params=params,
        timeout=10,
    )
    r.raise_for_status()
    payload = r.json()
    return len(payload.get("data", [])) > 0


def log_alert_failure(context):
    ti = context.get("ti")
    dag_run = context.get("dag_run")
    exc = context.get("exception")

    log.error(
        "ALERT_FAILURE dag_id=%s task_id=%s try_number=%s run_id=%s exception=%s",
        ti.dag_id if ti else None,
        ti.task_id if ti else None,
        ti.try_number if ti else None,
        dag_run.run_id if dag_run else None,
        repr(exc),
    )

default_args = {
    "owner": "x-lerasim",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": log_alert_failure,
}

with DAG(
    dag_id="extract_dag",
    start_date=datetime(2026, 4, 6),
    schedule_interval="0 0 * * *",
    default_args=default_args,
    tags=["extract", "postgres_vfd", "x-lerasim"],
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),

) as dag:

    start = EmptyOperator(task_id="start")

    ingest_postgres_vfd = SparkSubmitOperator(
        task_id="postgres_vfd",
        application=f"{SPARK_JOBS_PATH}/extract_pg.py",
        conn_id="spark_default",
        conf={"spark.master": "spark://spark-master:7077",
            "spark.jars.packages": "org.postgresql:postgresql:42.7.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
            },
        application_args=["--data-date", "{{ ds }}"],
        execution_timeout=timedelta(hours=1),
        verbose=True,
    )
   
    check_api_has_data = ShortCircuitOperator(
        task_id="check_api_has_data",
        python_callable=should_run_api
    )

    ingest_tracking_api = PythonOperator(
        task_id="ingest_tracking_api",
        python_callable=ingest_tms_api,
        op_kwargs={"data_date": "{{ ds }}"},
        execution_timeout=timedelta(minutes=20),
    )

    start >> ingest_postgres_vfd
    start >> check_api_has_data >>ingest_tracking_api