from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def test_python_environment():
    print("Airflow successfully running Python tasks in Docker environment!")


with DAG(
    dag_id="00_environment_test",
    default_args=default_args,
    description="Test DAG for Airflow environment verification",
    schedule_interval=None,  
    start_date=datetime(2026, 10, 16),
    catchup=False,
    tags=["test", "f1"],
) as dag:

    start_task = BashOperator(
        task_id="bash_ping",
        bash_command="echo 'Airflow Scheduler and Worker are running properly!'",
    )

    python_task = PythonOperator(
        task_id="python_ping",
        python_callable=test_python_environment,
    )

    start_task >> python_task