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
    SparkSession for Kafka streaming into Iceberg on MinIO using Hadoop S3A.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        # Kafka connector
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        # Iceberg Hadoop catalog pointing to MinIO via s3a
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", "s3a://warehouse/")
        # Hadoop S3A configs for MinIO
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Checkpointing for streaming
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints")
        .getOrCreate()
    )
    return spark


def start_kafka_streaming_job():
    spark = create_spark_session()

    # Create the Iceberg table if it doesn’t exist
    spark.sql("""
    CREATE TABLE IF NOT EXISTS local.db.vehicle_positions (
        key STRING,
        value STRING,
        timestamp TIMESTAMP
    )
    USING iceberg
    """)

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
        .toTable("local.db.vehicle_positions")
    )

    query.awaitTermination()


if __name__ == "__main__":
    start_kafka_streaming_job()

