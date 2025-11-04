# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col

# def create_spark_session(app_name="KafkaSparkStreaming", master="spark://spark-master:7077"):
#     """
#     Creates and configures a SparkSession for Kafka streaming.
#     """
#     spark = (
#         SparkSession.builder
#         .appName(app_name)
#         .master(master)
#         # Add Kafka connector package
#         .config(
#             "spark.jars.packages",
#             "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
#         )
#         # Sometimes needed if running in Docker/remote cluster
#         # .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints")
#         .getOrCreate()
#     )
#     return spark


# def start_kafka_streaming_job():
#     spark = create_spark_session()

#     # Read from Kafka topic
#     kafka_df = (
#         spark.readStream
#         .format("kafka")
#         .option("kafka.bootstrap.servers", "kafka:9092")
#         .option("subscribe", "vehicle_positions")
#         .option("startingOffsets", "earliest")  # ensures you get data from beginning
#         .load()
#     )

#     # Extract key/value as strings
#     processed_df = kafka_df.select(
#         col("key").cast("string").alias("key"),
#         col("value").cast("string").alias("value"),
#         col("timestamp")
#     )

#     # Write to console (demo only)
#     query = (
#         processed_df.writeStream
#         .outputMode("append")
#         .format("console")
#         .option("truncate", "false")
#         .start()
#     )

#     query.awaitTermination()


# if __name__ == "__main__":
#     start_kafka_streaming_job()


from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def create_spark_session(app_name="KafkaSparkStreaming", master="spark://spark-master:7077"):
    """
    Creates and configures a SparkSession for Kafka streaming with Iceberg + MinIO.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        # Kafka connector
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        # Iceberg catalog config
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", "s3://warehouse/")
        # MinIO S3 settings
        .config("spark.sql.catalog.local.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.local.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.local.s3.access-key-id", "minio")
        .config("spark.sql.catalog.local.s3.secret-access-key", "minio123")
        .config("spark.sql.catalog.local.s3.path-style-access", "true")
        # Checkpointing for streaming
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints")
        .getOrCreate()
    )
    return spark


def start_kafka_streaming_job():
    spark = create_spark_session()

    # Read from Kafka topic
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "vehicle_positions")
        .option("startingOffsets", "earliest")
        .load()
    )

    # Extract key/value as strings
    processed_df = kafka_df.select(
        col("key").cast("string").alias("key"),
        col("value").cast("string").alias("value"),
        col("timestamp")
    )

    # Write to Iceberg table in MinIO
    query = (
        processed_df.writeStream
        .format("iceberg")
        .outputMode("append")
        .option("path", "local.db.vehicle_positions")  # Iceberg table identifier
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    start_kafka_streaming_job()
