import boto3
import json
import random
import time
from datetime import datetime

# -----------------------------
# MinIO connection configuration
# -----------------------------
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",   # MinIO API endpoint
    aws_access_key_id="minio",              # From docker-compose.yml
    aws_secret_access_key="minio123"        # From docker-compose.yml
)

bucket_name = "raw"

# Create bucket if it doesn't exist
try:
    s3.create_bucket(Bucket=bucket_name)
    print(f"Created bucket: {bucket_name}")
except s3.exceptions.BucketAlreadyOwnedByYou:
    print(f"Bucket '{bucket_name}' already exists")

# -----------------------------
# Generate & upload fake sensor data
# -----------------------------
for i in range(10):  # 10 fake records
    record = {
        "timestamp": datetime.utcnow().isoformat(),
        "sensor_id": random.randint(1, 5),
        "value": round(random.uniform(20.0, 30.0), 2)
    }

    # File name for each record
    file_name = f"sensor_{i}.json"

    # Upload to MinIO
    s3.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=json.dumps(record).encode("utf-8")
    )

    print(f"Uploaded {file_name} → {record}")
    time.sleep(1)
