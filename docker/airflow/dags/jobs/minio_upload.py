
import os
import logging
import boto3
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def upload_file_to_minio(file_path, bucket_name, object_name=None):
    """Upload a file to an S3 bucket (Minio in this case)

    :param file_path: Path to file to upload
    :param bucket_name: S3 bucket to upload to
    :param object_name: S3 object name. If not specified then file_path is used
    :return: True if file was uploaded, else False
    """
    if object_name is None:
        object_name = os.path.basename(file_path)

    # Minio connection details from environment variables
    minio_endpoint = os.getenv("MINIO_ENDPOINT")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY")

    if not all([minio_endpoint, minio_access_key, minio_secret_key]):
        logger.error("Minio credentials or endpoint not set in environment variables.")
        return False

    # Create an S3 client
    s3_client = boto3.client(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key,
        config=boto3.session.Config(signature_version='s3v4')
    )

    try:
        # Check if the bucket exists, if not, create it
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            logger.info(f"Bucket '{bucket_name}' already exists.")
        except ClientError:
            logger.info(f"Bucket '{bucket_name}' does not exist. Creating it...")
            s3_client.create_bucket(Bucket=bucket_name)
            logger.info(f"Bucket '{bucket_name}' created successfully.")

        s3_client.upload_file(file_path, bucket_name, object_name)
        logger.info(f"File '{file_path}' uploaded to '{bucket_name}/{object_name}' successfully.")
    except ClientError as e:
        logger.error(f"Error uploading file to Minio: {e}")
        return False
    return True

if __name__ == "__main__":
    # Example usage (for testing purposes)
    # These would typically be passed as arguments in an Airflow DAG
    test_file_path = "/path/to/your/local/file.txt" # Replace with a dummy file for local testing
    test_bucket_name = "test-bucket"
    test_object_name = "test-file.txt"

    # Set dummy environment variables for local testing if not already set
    os.environ["MINIO_ENDPOINT"] = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    os.environ["MINIO_ACCESS_KEY"] = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    os.environ["MINIO_SECRET_KEY"] = os.getenv("MINIO_SECRET_KEY", "minioadmin")

    if os.path.exists(test_file_path):
        upload_file_to_minio(test_file_path, test_bucket_name, test_object_name)
    else:
        logger.warning(f"Test file '{test_file_path}' not found. Skipping local test upload.")
        logger.info("Please create a dummy file or update 'test_file_path' for local testing.")
