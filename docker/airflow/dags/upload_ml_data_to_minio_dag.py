from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import sys

# Add the jobs directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'jobs')))
from minio_upload import upload_file_to_minio

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id='upload_ml_data_to_minio',
    default_args=default_args,
    description='Upload movies.dat and users.dat to Minio Raw_data bucket',
    schedule=None, # This DAG can be triggered manually or on a schedule
    catchup=False,
    tags=['minio', 'upload', 'ml-1m'],
) as dag:

    # Define the base path to the data files relative to the DAGs folder
    # This assumes the 'Data' folder is at the project root level
    base_data_path = "/opt/airflow/project_data"

    upload_movies_task = PythonOperator(
        task_id='upload_movies_dat',
        python_callable=upload_file_to_minio,
        op_kwargs={
            'file_path': os.path.join(base_data_path, 'movies.dat'),
            'bucket_name': 'raw-data',
            'object_name': 'movies.dat',
        },
    )

    upload_users_task = PythonOperator(
        task_id='upload_users_dat',
        python_callable=upload_file_to_minio,
        op_kwargs={
            'file_path': os.path.join(base_data_path, 'users.dat'),
            'bucket_name': 'raw-data',
            'object_name': 'users.dat',
        },
    )

    # Set task dependencies
    upload_movies_task >> upload_users_task
