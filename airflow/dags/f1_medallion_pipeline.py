import os
import sys
from datetime import datetime, timedelta

sys.path.append("/opt/airflow")

from airflow import DAG
from airflow.decorators import task

from f1_data_pipeline.bronze_ingestion import ingest_bronze_data

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="f1_medallion_pipeline",
    default_args=default_args,
    description="Formula 1 ETL Pipeline using Medallion Architecture (Bronze -> Silver -> Gold)",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["f1", "medallion"],
) as dag:

    @task(task_id="ingest_bronze_layer")
    def run_bronze_ingestion():
        ingest_bronze_data(file_path="/opt/airflow/data/dataEngineeringDataset.csv")

    run_bronze_ingestion()