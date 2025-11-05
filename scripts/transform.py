import boto3, os, tempfile
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, date_format

# -------------------------------
# Config
# -------------------------------
MINIO_ENDPOINT = "http://20.167.18.24:9000"
ACCESS_KEY = "minio"
SECRET_KEY = "minio123"
RAW_BUCKET = "minio"
PROCESSED_BUCKET = "minio"

LOCAL_RAW_DIR = tempfile.mkdtemp(prefix="raw_")
LOCAL_PROCESSED_DIR = tempfile.mkdtemp(prefix="processed_")

# -------------------------------
# Connect to MinIO
# -------------------------------
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)

# -------------------------------
# Download Raw Files from MinIO
# -------------------------------
objects = s3.list_objects_v2(Bucket=RAW_BUCKET, Prefix="raw/").get("Contents", [])
for obj in objects:
    key = obj["Key"]
    if key.endswith(".json"):
        local_path = os.path.join(LOCAL_RAW_DIR, os.path.basename(key))
        s3.download_file(RAW_BUCKET, key, local_path)
        print(f"⬇️ Downloaded {key}")

# -------------------------------
# Process with Spark
# -------------------------------
spark = SparkSession.getActiveSession()
df = spark.read.option("multiline", "true").json(LOCAL_RAW_DIR)

df = (
    df.withColumn("event_time", to_timestamp("Datetime"))
      .withColumn("date", date_format("event_time", "yyyy-MM-dd"))
)

cols = [c for c in ["symbol", "event_time", "date", "Open", "High", "Low", "Close", "Volume"] if c in df.columns]
df = df.select(*cols)

df.write.mode("overwrite").parquet(LOCAL_PROCESSED_DIR)
print(f"✅ Spark processed data saved locally: {LOCAL_PROCESSED_DIR}")

# -------------------------------
# Upload Processed Parquet Back to MinIO
# -------------------------------
for root, _, files in os.walk(LOCAL_PROCESSED_DIR):
    for file in files:
        path = os.path.join(root, file)
        key = f"processed/{file}"
        s3.upload_file(path, PROCESSED_BUCKET, key)
        print(f"⬆️ Uploaded {key}")

print("✅ ETL complete — data processed and reuploaded to MinIO.")
