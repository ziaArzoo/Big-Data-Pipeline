import boto3
from botocore.config import Config
import os
#class to connect with minio saperated from main code
def get_minio_client():
    """Create and return a boto3 client for MinIO."""
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123")

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
    )
    return s3

def ensure_bucket(s3, bucket_name):
    """Ensure a bucket exists, else create it."""
    existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if bucket_name not in existing:
        s3.create_bucket(Bucket=bucket_name)
        print(f"🪣 Created bucket: {bucket_name}")
    return True


def clear_bucket(s3, bucket_name):
    """Delete all objects inside a bucket."""
    try:
        objects = s3.list_objects_v2(Bucket=bucket_name).get("Contents", [])
        if not objects:
            print(f"No existing files found in '{bucket_name}'")
            return
        keys = [{"Key": obj["Key"]} for obj in objects]
        s3.delete_objects(Bucket=bucket_name, Delete={"Objects": keys})
        print(f" Cleared {len(keys)} files from '{bucket_name}'")
    except Exception as e:
        print(f" Could not clear bucket '{bucket_name}': {e}")


