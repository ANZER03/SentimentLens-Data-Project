import os
import pytest
from pyspark.sql import SparkSession
from minio import Minio
import os

from src.processing.process_users import process_users_data
from src.utils.spark_factory import create_spark_session

@pytest.fixture(scope="session")
def spark_session():
    """Create a Spark session for testing."""
    os.environ["MINIO_ENDPOINT"] = "http://localhost:9000"
    os.environ["MINIO_ACCESS_KEY"] = "minioadmin"
    os.environ["MINIO_SECRET_KEY"] = "minioadmin"
    os.environ["ICEBERG_CATALOG_URI"] = "jdbc:postgresql://localhost:5432/iceberg_catalog"
    os.environ["ICEBERG_JDBC_USER"] = "iceberg"
    os.environ["ICEBERG_JDBC_PASSWORD"] = "iceberg"
    return create_spark_session()

@pytest.fixture(scope="session")
def minio_client():
    """Create a Minio client for testing."""
    return Minio(
        "localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

def test_process_users_data(spark_session, minio_client):
    """Test the process_users_data function."""
    # Create a dummy users.dat file
    bucket_name = "ml-1m"
    object_name = "users.dat"
    file_path = "/tmp/users.dat"
    with open(file_path, "w") as f:
        f.write("1::F::1::10::48067\n")
        f.write("2::M::56::16::70072\n")

    # Upload the file to Minio
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
    minio_client.fput_object(bucket_name, object_name, file_path)

    # Process the data
    process_users_data(spark_session)

    # Read the data from the Iceberg table
    users_df = spark_session.read.table("iceberg.users")

    # Assert the data is correct
    assert users_df.count() == 2
    assert users_df.columns == ["user_id", "gender", "age", "occupation", "zip_code"]

    # Clean up
    os.remove(file_path)
    minio_client.remove_object(bucket_name, object_name)
    minio_client.remove_bucket(bucket_name)
