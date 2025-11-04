from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="spark_pi_direct",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    description="Run SparkPi example directly with spark:// URL",
) as dag:

    spark_pi = SparkSubmitOperator(
        task_id="run_spark_pi_direct",
        application="/opt/bitnami/spark/examples/jars/spark-examples_2.12-3.5.1.jar",
        java_class="org.apache.spark.examples.SparkPi",
        name="airflow-spark-pi",
        application_args=["100"],
        conf={
            "spark.master": "spark://spark-master:7077"   # ✅ pass master here
        },
        verbose=True,
    )

# from datetime import datetime
# from airflow import DAG
# from airflow.operators.bash import BashOperator

# with DAG(
#     dag_id="spark_pi_bash",
#     start_date=datetime(2024, 1, 1),
#     schedule=None,
#     catchup=False,
# ) as dag:

#     run_pi = BashOperator(
#         task_id="run_spark_pi",
#         bash_command=(
#             "docker exec spark-master "
#             "spark-submit --master spark://spark-master:7077 "
#             "--class org.apache.spark.examples.SparkPi "
#             "/opt/bitnami/spark/examples/jars/spark-examples_2.12-3.5.1.jar 100"
#         ),
#     )