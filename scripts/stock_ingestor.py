import os
import json
import datetime
import time
import yfinance as yf
import boto3
from botocore.config import Config
import pandas as pd

# -------------------------------
# Configuration
# -------------------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123")
BUCKET = os.getenv("MINIO_BUCKET", "raw")   # your actual bucket name

SYMBOLS = ["AAPL", "MSFT", "GOOG"]
PERIOD = "30d"
INTERVAL = "1h"

# -------------------------------
# MinIO Client
# -------------------------------
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version="s3v4"),
)

def fetch_and_upload():
    # Ensure bucket exists
    existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if BUCKET not in existing:
        s3.create_bucket(Bucket=BUCKET)
        print(f"Created bucket: {BUCKET}")

    for symbol in SYMBOLS:
        print(f"\n🔹 Fetching {symbol} ...")
        df = yf.download(symbol, period=PERIOD, interval=INTERVAL,
                         progress=False, auto_adjust=False)
        time.sleep(1)  # to avoid rate limits

        if df.empty:
            print(f"⚠️ No data for {symbol}")
            continue

        # Reset index and flatten any multiindex columns
        df = df.reset_index()
        df.columns = ['_'.join(c) if isinstance(c, tuple) else c for c in df.columns]
        df["symbol"] = symbol

        # Convert datetime to string for JSON
        for col in df.columns:
            if isinstance(df[col].iloc[0], (pd.Timestamp, datetime.datetime)):
                df[col] = df[col].astype(str)

        # Convert to records
        records = df.to_dict(orient="records")

        # Create timestamped filename for this symbol
        timestamp = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
        file_name = f"{symbol}_stock_raw_{timestamp}.json"

        # Custom encoder for timestamps
        class EnhancedJSONEncoder(json.JSONEncoder):
            def default(self, o):
                if isinstance(o, (pd.Timestamp, datetime.datetime)):
                    return o.isoformat()
                return super().default(o)

        body = json.dumps(records, cls=EnhancedJSONEncoder, ensure_ascii=False)

        # Upload to bucket root
        s3.put_object(Bucket=BUCKET, Key=file_name, Body=body.encode("utf-8"))
        print(f"✅ Uploaded {file_name} to bucket '{BUCKET}' ({len(records)} records)")

if __name__ == "__main__":
    fetch_and_upload()
