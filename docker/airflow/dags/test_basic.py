from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="test_basic",
    start_date=datetime(2024, 1, 1),
    schedule=None,   # replaces schedule_interval
    catchup=False,
    description="Minimal DAG to verify scheduler/webserver",
) as dag:

    hello = BashOperator(
        task_id="hello",
        bash_command='echo "Airflow is alive: $(date)"'
    )

    list_dir = BashOperator(
        task_id="list_dir",
        bash_command="ls -la /opt/airflow/dags"
    )

    hello >> list_dir
