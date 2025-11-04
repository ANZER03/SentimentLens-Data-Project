
from pyspark.sql import SparkSession

def create_spark_session(app_name="MovieLensLakehouse", config=None):
    """
    Creates a Spark session with Iceberg and Minio integration.
    """
    if config is None:
        config = {}

    # Default configurations for Iceberg and Minio
    spark_builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4,software.amazon.awssdk:bundle:2.17.178")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hadoop")
        .config("spark.sql.catalog.spark_catalog.warehouse", config.get("iceberg_warehouse_path", "s3a://warehouse/"))
        .config("spark.hadoop.fs.s3a.endpoint", config.get("minio_endpoint", "http://localhost:9000"))
        .config("spark.hadoop.fs.s3a.access.key", config.get("minio_access_key", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", config.get("minio_secret_key", "minioadmin"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    )

    # Apply any additional configurations
    for key, value in config.items():
        spark_builder = spark_builder.config(key, value)

    return spark_builder.getOrCreate()

