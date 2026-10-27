import os
import sys
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.docker.operators.docker import DockerOperator

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
if AIRFLOW_HOME not in sys.path:
    sys.path.append(AIRFLOW_HOME)

from f1_data_pipeline.bronze_ingestion import ingest_bronze_data
# Importuj novu funkciju ovdje
from f1_data_pipeline.silver_transformations import transform_silver_layer, get_highest_watermark
from f1_data_pipeline.gold_transformations import transform_gold_layer

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")

default_args = {
    'owner': 'f1_data_engineer',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

@dag(
    dag_id='f1_medallion_pipeline',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval='0 0 * * 6,0',  
    catchup=False,
    tags=['f1', 'medallion', 'elt', 'docker']
)
def f1_medallion_pipeline():

    @task()
    def ingest_bronze_csv():
        ingest_bronze_data("/opt/airflow/data/dataEngineeringDataset.csv")

    @task()
    def get_last_processed_race() -> dict:
        return get_highest_watermark()

    produce_api_data = DockerOperator(
        task_id='produce_api_data',
        image='f1-app:latest',
        command='python f1_data_pipeline/kafka/producer.py --start-season {{ ti.xcom_pull(task_ids="get_last_processed_race")["season"] }} --start-round {{ ti.xcom_pull(task_ids="get_last_processed_race")["round"] }}',
        network_mode='f1_network', 
        docker_url='unix://var/run/docker.sock',
        auto_remove=True,
        mount_tmp_dir=False,
        environment={
            'KAFKA_BOOTSTRAP_SERVERS': 'kafka:29092'
        }
    )

    consume_api_data = DockerOperator(
        task_id='consume_api_data',
        image='f1-app:latest',
        command='python f1_data_pipeline/kafka/consumer.py',
        network_mode='f1_network', 
        docker_url='unix://var/run/docker.sock',
        auto_remove=True,
        mount_tmp_dir=False,
        environment={
            'KAFKA_BOOTSTRAP_SERVERS': 'kafka:29092',
            'POSTGRES_HOST': 'f1_postgres',  
            'POSTGRES_USER': POSTGRES_USER,
            'POSTGRES_PASSWORD': POSTGRES_PASSWORD,
            'POSTGRES_DB': POSTGRES_DB
        }
    )

    @task()
    def transform_silver():
        transform_silver_layer()
        
    @task()
    def transform_gold():
        transform_gold_layer()

    csv_task = ingest_bronze_csv()
    
    watermark_task = get_last_processed_race()
    api_task = watermark_task >> produce_api_data >> consume_api_data

    [csv_task, api_task] >> transform_silver() >> transform_gold()

f1_pipeline_dag = f1_medallion_pipeline()