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
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.postgresql:postgresql:42.5.4")
        # Iceberg JDBC catalog pointing to PostgreSQL
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "jdbc")
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://warehouse/")
        .config("spark.sql.catalog.iceberg.uri", "jdbc:postgresql://postgres:5432/iceberg_catalog")
        .config("spark.sql.catalog.iceberg.jdbc.user", "iceberg")
        .config("spark.sql.catalog.iceberg.jdbc.password", "iceberg")
        # .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        # Hadoop S3A configs for MinIO
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Additional Iceberg optimizations
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.defaultCatalog", "iceberg")
        # Checkpointing for streaming
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints")
        .getOrCreate()
    )

    # spark = (
    #     SparkSession.builder
    #     .appName(app_name)
    #     .master(master)
    #     # Kafka connector
    #     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
    #     # Iceberg Hadoop catalog pointing to MinIO via s3a
    #     .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    #     .config("spark.sql.catalog.local.type", "hadoop")
    #     .config("spark.sql.catalog.local.warehouse", "s3a://warehouse/")
    #     # Hadoop S3A configs for MinIO
    #     .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    #     .config("spark.hadoop.fs.s3a.access.key", "minio")
    #     .config("spark.hadoop.fs.s3a.secret.key", "minio123")
    #     .config("spark.hadoop.fs.s3a.path.style.access", "true")
    #     .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    #     # Checkpointing for streaming
    #     .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints")
    #     .getOrCreate()
    # )
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
        .toTable("db_gold.vehicle_positions_stream")
    )

    query.awaitTermination()


if __name__ == "__main__":
    start_kafka_streaming_job()
