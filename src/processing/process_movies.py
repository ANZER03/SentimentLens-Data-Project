from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from utils import spark_factory

def process_movies_data(spark: SparkSession):
    """
    Reads movies data from Minio, processes it, and writes it to an Iceberg table.
    """
    
    schema = StructType([
        StructField("movie_id", IntegerType(), True),
        StructField("title", StringType(), True),
        StructField("genres", StringType(), True)
    ])

    # Read data from Minio
    movies_df = spark.read.format("csv") \
        .option("delimiter", "::") \
        .schema(schema) \
        .load("s3a://raw-data/movies.dat")


    # Write data to Iceberg
    movies_df.write.mode("overwrite").saveAsTable("iceberg.silver_layer.movies")

if __name__ == "__main__":
    # Create Spark session
    spark = spark_factory.create_spark_session("process_movies")

    # Process data
    process_movies_data(spark)

    # Stop Spark session
    spark.stop()
