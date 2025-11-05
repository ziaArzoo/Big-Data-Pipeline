import os
import json
import datetime
import time
import yfinance as yf
import pandas as pd
from minio_utils import get_minio_client, ensure_bucket, clear_bucket

# -------------------------------
# Config
# -------------------------------
BUCKET = os.getenv("MINIO_BUCKET", "raw")
SYMBOLS = ["AAPL", "MSFT", "GOOG"]
PERIOD = "30d"
INTERVAL = "1h"

# -------------------------------
# Ingestion Logic
# -------------------------------
def fetch_and_upload():
    s3 = get_minio_client()
    ensure_bucket(s3, BUCKET)
    clear_bucket(s3, BUCKET)  # 🧹 cleanup before fresh ingest

    for symbol in SYMBOLS:
        print(f"\n🔹 Fetching {symbol} ...")
        df = yf.download(symbol, period=PERIOD, interval=INTERVAL, progress=False)
        time.sleep(1)

        if df.empty:
            print(f"⚠️ No data for {symbol}")
            continue

        df = df.reset_index()
        df.columns = ['_'.join(c) if isinstance(c, tuple) else c for c in df.columns]
        df["symbol"] = symbol

        # Convert datetime columns to string for JSON
        for col in df.columns:
            if isinstance(df[col].iloc[0], (pd.Timestamp, datetime.datetime)):
                df[col] = df[col].astype(str)

        records = df.to_dict(orient="records")
        timestamp = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
        file_name = f"{symbol}_stock_raw_{timestamp}.json"

        body = json.dumps(records, ensure_ascii=False)
        s3.put_object(Bucket=BUCKET, Key=file_name, Body=body.encode("utf-8"))
        print(f"✅ Uploaded {file_name} ({len(records)} records)")

if __name__ == "__main__":
    fetch_and_upload()
