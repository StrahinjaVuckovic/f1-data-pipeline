import os
import sys
from datetime import datetime, timedelta
from airflow.decorators import dag, task

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
if AIRFLOW_HOME not in sys.path:
    sys.path.append(AIRFLOW_HOME)

from f1_data_pipeline.bronze_ingestion import ingest_bronze_data
from f1_data_pipeline.silver_transformations import transform_silver_layer

default_args = {
    'owner': 'f1_data_engineer',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='f1_medallion_pipeline',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['f1', 'medallion', 'elt']
)
def f1_medallion_pipeline():

    @task()
    def ingest_bronze():
        ingest_bronze_data("/opt/airflow/data/dataEngineeringDataset.csv")

    @task()
    def transform_silver():
        transform_silver_layer()

    ingest_bronze() >> transform_silver()

f1_pipeline_dag = f1_medallion_pipeline()