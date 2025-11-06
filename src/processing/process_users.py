from pyspark.sql import SparkSession

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from utils import spark_factory

def process_users_data(spark: SparkSession):
    """
    Reads user data from Minio, processes it, and writes it to an Iceberg table.
    """
    # Read data from Minio
    users_df = spark.read.format("csv").option("delimiter", "::").load("s3a://raw-data/users.dat")

    # Rename columns
    users_df = users_df.withColumnRenamed("_c0", "user_id") \
                       .withColumnRenamed("_c1", "gender") \
                       .withColumnRenamed("_c2", "age") \
                       .withColumnRenamed("_c3", "occupation") \
                       .withColumnRenamed("_c4", "zip_code")

    # Write data to Iceberg
    users_df.write.mode("overwrite").saveAsTable("iceberg.silver_layer.users")

if __name__ == "__main__":
    # Create Spark session
    spark = spark_factory.create_spark_session()

    # Process data
    process_users_data(spark)

    # Stop Spark session
    spark.stop()
