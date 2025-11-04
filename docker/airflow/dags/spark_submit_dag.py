from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {"owner": "airflow", "start_date": datetime(2024, 1, 1)}

with DAG(
    dag_id="spark_submit_test",
    default_args=default_args,
    schedule=None,
    catchup=False,
) as dag:

    spark_task = SparkSubmitOperator(
        task_id="spark_wordcount",
        application="/opt/airflow/dags/jobs/wordcount.py",  # must exist in Airflow container
        conn_id="spark_default",
        name="airflow-spark-job",
        verbose=True,  # driver runs in Airflow
    )