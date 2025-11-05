import os
import boto3
from botocore.config import Config
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lag, avg, to_timestamp, date_format, round as _round
)
from pyspark.sql.window import Window
import shutil

# -------------------------------
# Configuration
# -------------------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123")
BUCKET = os.getenv("MINIO_BUCKET", "raw")     # raw bucket
PROCESSED_BUCKET = os.getenv("PROCESSED_BUCKET", "processed")  # processed bucket

LOCAL_STAGE = Path("/tmp/spark_stage")
LOCAL_STAGE.mkdir(parents=True, exist_ok=True)

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version="s3v4"),
)

# Ensure processed bucket exists
existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
if PROCESSED_BUCKET not in existing:
    s3.create_bucket(Bucket=PROCESSED_BUCKET)
    print(f"Created bucket: {PROCESSED_BUCKET}")

# -------------------------------
# Download raw JSONs from MinIO
# -------------------------------
resp = s3.list_objects_v2(Bucket=BUCKET)
objects = [o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".json")]
if not objects:
    raise SystemExit("❌ No raw files found in MinIO")

local_files = []
for obj in objects:
    dest = LOCAL_STAGE / Path(obj).name
    s3.download_file(BUCKET, obj, str(dest))
    local_files.append(str(dest))
print(f"✅ Downloaded {len(local_files)} raw JSON files for ETL")

# -------------------------------
# Spark Session
# -------------------------------
spark = (
    SparkSession.builder.appName("StockETL")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", True)
    .getOrCreate()
)

# -------------------------------
# Read + Transform
# -------------------------------
df = spark.read.option("multiline", "true").json(local_files)
df = (
    df.withColumn("event_time", to_timestamp(col("Datetime")))
      .withColumn("date", date_format(col("event_time"), "yyyy-MM-dd"))
)

# compute percent change & rolling avg
window = Window.partitionBy("symbol").orderBy("event_time")
df = (
    df.withColumn("pct_change", _round((col("Close") - lag("Close").over(window)) / lag("Close").over(window) * 100, 2))
      .withColumn("ma_5", _round(avg("Close").over(window.rowsBetween(-4, 0)), 2))
)

# keep only essential columns
selected_cols = ["symbol", "event_time", "date", "Open", "High", "Low", "Close", "Volume", "pct_change", "ma_5"]
df = df.select(*[c for c in selected_cols if c in df.columns])

# -------------------------------
# Write to Parquet (partitioned)
# -------------------------------
out_dir = LOCAL_STAGE / "processed"
df.repartition("symbol", "date").write.mode("overwrite").partitionBy("symbol", "date").parquet(str(out_dir))
print("✅ Parquet written locally")

# -------------------------------
# Upload to MinIO processed bucket
# -------------------------------
for p in out_dir.rglob("*.parquet"):
    key = f"{p.relative_to(out_dir)}".replace("\\", "/")
    s3.upload_file(str(p), PROCESSED_BUCKET, key)
print(f"✅ Uploaded processed Parquet files to '{PROCESSED_BUCKET}'")

# cleanup
shutil.rmtree(LOCAL_STAGE, ignore_errors=True)
print("🏁 ETL complete.")
