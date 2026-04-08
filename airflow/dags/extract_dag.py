from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from api_ingestion.api_ingestion import (
    ingest_all_pages,
    load_watermark,
    max_timestamp,
    save_watermark,
    upload_to_minio,
)

SPARK_JOBS_PATH = "/opt/spark/jobs"


def ingest_tms_api():
    wm = load_watermark()
    if wm is None and os.getenv("TRACKING_INITIAL_SINCE"):
        wm = os.getenv("TRACKING_INITIAL_SINCE")

    events = ingest_all_pages(since=wm)

    if not events:
        return

    upload_to_minio(events)
    m = max_timestamp(events)
    if m:
        save_watermark(m)


default_args = {
    "owner": "x-lerasim",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="extract_dag",
    start_date=datetime(2026, 4, 7),
    schedule_interval="0 0 * * *",
    default_args=default_args,
    tags=["extract", "postgres_kmk", "x-lerasim"],
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
) as dag:

    start = EmptyOperator(task_id="start")

    ingest_postgres_kmk = SparkSubmitOperator(
        task_id="postgres_kmk",
        application=f"{SPARK_JOBS_PATH}/extract_pg.py",
        conn_id="spark_default",
        conf={"spark.master": "spark://spark-master:7077",
            "spark.jars.packages": "org.postgresql:postgresql:42.7.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
            },
        application_args=["--data-date", "{{ ds }}"],
        execution_timeout=timedelta(hours=1),
        verbose=True,
    )

    ingest_tracking_api = PythonOperator(
        task_id="ingest_tracking_api",
        python_callable=ingest_tms_api,
        execution_timeout=timedelta(minutes=20),
    )

    start >> [ingest_postgres_kmk, ingest_tracking_api]
