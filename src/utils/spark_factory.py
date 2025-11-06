import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()

def create_spark_session(app_name="MovieLensLakehouse", config=None):
    """
    Creates a Spark session with Iceberg and Minio integration.
    """
    if config is None:
        config = {}

    # Minio and Iceberg JDBC catalog details from environment variables
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minio")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minio123")
    iceberg_catalog_uri = os.getenv("ICEBERG_CATALOG_URI", "jdbc:postgresql://localhost:5432/iceberg_catalog")
    iceberg_jdbc_user = os.getenv("ICEBERG_JDBC_USER", "iceberg")
    iceberg_jdbc_password = os.getenv("ICEBERG_JDBC_PASSWORD", "iceberg")


    spark_builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.postgresql:postgresql:42.5.4")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "jdbc")
        .config("spark.sql.catalog.iceberg.uri", "jdbc:postgresql://postgres:5432/iceberg_catalog")
        .config("spark.sql.catalog.iceberg.jdbc.user", "iceberg")
        .config("spark.sql.catalog.iceberg.jdbc.password", "iceberg")
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://warehouse/")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.defaultCatalog", "iceberg")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints")

    )

    # Apply any additional configurations
    for key, value in config.items():
        spark_builder = spark_builder.config(key, value)

    return spark_builder.getOrCreate()
