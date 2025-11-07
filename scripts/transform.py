import json
import io
import polars as pl
from datetime import datetime
from minio_utils import get_minio_client, ensure_bucket, clear_bucket

BUCKET_RAW = "raw"
BUCKET_PROCESSED = "processed"
SYMBOLS = ["AAPL", "MSFT", "GOOG"]

def get_latest_files(s3):
    """Fetch latest JSON file for each stock symbol."""
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


def transform_and_upload():
    s3 = get_minio_client()
    ensure_bucket(s3, BUCKET_PROCESSED)
    clear_bucket(s3, BUCKET_PROCESSED)

    latest_files = get_latest_files(s3)
    if not latest_files:
        raise Exception("No files found to process.")

    for symbol, meta in latest_files.items():
        key = meta["Key"]
        print(f" Reading {key}")

        data = s3.get_object(Bucket=BUCKET_RAW, Key=key)["Body"].read()
        records = json.loads(data)
        df = pl.DataFrame(records)

        dt_col = next((c for c in df.columns if "datetime" in c.lower()), None)
        if dt_col:
            df = df.with_columns([
                pl.col(dt_col).str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False).alias("Datetime")
            ])

        # Normalize columns — remove suffix like _AAPL
        rename_map = {}
        for col in df.columns:
            for metric in ["Open", "High", "Low", "Close", "Volume"]:
                if metric.lower() in col.lower():
                    rename_map[col] = metric
        df = df.rename(rename_map)

        # Add symbol if not present
        if "symbol" not in df.columns:
            df = df.with_columns(pl.lit(symbol).alias("symbol"))

        # Select final clean schema
        cols_to_keep = [c for c in ["Datetime", "Open", "High", "Low", "Close", "Volume", "symbol"] if c in df.columns]
        df = df.select(cols_to_keep)
        print(f" Cleaned columns for {symbol}: {cols_to_keep}")

        # Write to MinIO
        buffer = io.BytesIO()
        df.write_parquet(buffer)
        timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
        key_out = f"{symbol}_processed_{timestamp}.parquet"
        s3.put_object(Bucket=BUCKET_PROCESSED, Key=key_out, Body=buffer.getvalue())
        print(f" Uploaded processed file for {symbol} → {key_out}")

    print(" All symbols processed successfully!")


if __name__ == "__main__":
    transform_and_upload()
