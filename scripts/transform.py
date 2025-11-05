import json
import io
import polars as pl
from datetime import datetime
from minio_utils import get_minio_client, ensure_bucket, clear_bucket

# -------------------------------
# Config
# -------------------------------
BUCKET_RAW = "raw"
BUCKET_PROCESSED = "processed"
SYMBOLS = ["AAPL", "MSFT", "GOOG"]

# -------------------------------
# Helper — get latest file per symbol
# -------------------------------
def get_latest_files(s3):
    """Fetch the latest JSON file for each stock symbol."""
    objs = s3.list_objects_v2(Bucket=BUCKET_RAW).get("Contents", [])
    if not objs:
        raise Exception("No raw files found!")

    latest = {}
    for obj in objs:
        key = obj["Key"]
        for symbol in SYMBOLS:
            if key.startswith(symbol):
                if symbol not in latest or obj["LastModified"] > latest[symbol]["LastModified"]:
                    latest[symbol] = obj
    return latest

# -------------------------------
# Transform Logic
# -------------------------------
def transform_and_upload():
    s3 = get_minio_client()
    ensure_bucket(s3, BUCKET_PROCESSED)

    # 🧹 Clear processed folder before writing new outputs
    clear_bucket(s3, BUCKET_PROCESSED)

    latest_files = get_latest_files(s3)
    if not latest_files:
        raise Exception("No files found to process.")

    for symbol, meta in latest_files.items():
        key = meta["Key"]
        print(f"📥 Reading {key}")

        data = s3.get_object(Bucket=BUCKET_RAW, Key=key)["Body"].read()
        records = json.loads(data)
        df = pl.DataFrame(records)

        # Detect datetime column
        dt_col = next((c for c in df.columns if "datetime" in c.lower()), None)
        if dt_col:
            df = df.with_columns([
                pl.col(dt_col).str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False).alias("Datetime")
            ])

        # Keep essential columns
        cols_to_keep = [c for c in ["Datetime", "Open", "High", "Low", "Close", "Volume", "symbol"] if c in df.columns]
        df = df.select(cols_to_keep)

        # 🧾 Write one file per stock
        buffer = io.BytesIO()
        df.write_parquet(buffer)
        timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
        key_out = f"{symbol}_processed_{timestamp}.parquet"
        s3.put_object(Bucket=BUCKET_PROCESSED, Key=key_out, Body=buffer.getvalue())
        print(f"✅ Uploaded processed file for {symbol} → {key_out}")

    print("🎯 All symbols processed successfully!")


# -------------------------------
# Entry Point
# -------------------------------
if __name__ == "__main__":
    transform_and_upload()
