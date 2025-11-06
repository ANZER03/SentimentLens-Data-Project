from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from utils import spark_factory

def process_users_data(spark: SparkSession):
    """
    Reads user data from Minio, processes it, and writes it to an Iceberg table.
    """
    schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("gender", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("occupation", IntegerType(), True),
        StructField("zip_code", StringType(), True)
    ])

    # Read data from Minio
    users_df = spark.read.format("csv") \
        .option("delimiter", "::") \
        .schema(schema) \
        .load("s3a://raw-data/users.dat")

    # Write data to Iceberg
    users_df.write.mode("overwrite").saveAsTable("iceberg.silver_layer.users")

if __name__ == "__main__":
    # Create Spark session
    spark = spark_factory.create_spark_session()

    # Process data
    process_users_data(spark)

    # Stop Spark session
    spark.stop()
