import os
import json
import datetime
import yfinance as yf
import boto3
from botocore.config import Config

# Environment configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123")
BUCKET = os.getenv("MINIO_BUCKET", "my-bucket")

# Select symbols and timeframe
SYMBOLS = ["AAPL", "MSFT", "GOOG"]
PERIOD = "30d"
INTERVAL = "1h"

# Connect to MinIO
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version="s3v4"),
)

def fetch_and_upload():
    all_data = []
    for symbol in SYMBOLS:
        df = yf.download(symbol, period=PERIOD, interval=INTERVAL, progress=False)
        if df.empty:
            print(f"No data found for {symbol}")
            continue
        records = df.reset_index().to_dict(orient="records")
        for rec in records:
            rec["symbol"] = symbol
        all_data.extend(records)

    if not all_data:
        print("No data fetched.")
        return

    timestamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    file_name = f"stock_raw_{timestamp}.json"
    body = json.dumps(all_data, default=str)

    s3.put_object(Bucket=BUCKET, Key=f"raw/{file_name}", Body=body)
    print(f"Uploaded {file_name} to MinIO/raw/ with {len(all_data)} records")

if __name__ == "__main__":
    fetch_and_upload()
