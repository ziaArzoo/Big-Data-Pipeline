import io
import pandas as pd
from datetime import datetime
from minio_utils import get_minio_client, ensure_bucket, clear_bucket

BUCKET_PROCESSED = "processed"
BUCKET_COMBINED = "combined"

def combine_processed():
    s3 = get_minio_client()
    ensure_bucket(s3, BUCKET_COMBINED)
    clear_bucket(s3, BUCKET_COMBINED)

    objs = s3.list_objects_v2(Bucket=BUCKET_PROCESSED).get("Contents", [])
    if not objs:
        raise Exception("❌ No processed files found!")

    dfs = []
    for obj in objs:
        key = obj["Key"]
        print(f"📥 Reading {key}")
        data = s3.get_object(Bucket=BUCKET_PROCESSED, Key=key)["Body"].read()
        df = pd.read_parquet(io.BytesIO(data))
        dfs.append(df)

    combined_df = pd.concat(dfs, ignore_index=True)
    print(f"✅ Combined {len(dfs)} files, total rows: {len(combined_df)}")

    buffer = io.BytesIO()
    combined_df.to_parquet(buffer, index=False)
    key_out = f"combined_{datetime.now().strftime('%Y%m%dT%H%M%S')}.parquet"
    s3.put_object(Bucket=BUCKET_COMBINED, Key=key_out, Body=buffer.getvalue())

    print(f"📤 Uploaded combined dataset → {key_out}")

if __name__ == "__main__":
    combine_processed()
