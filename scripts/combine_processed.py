import io
import polars as pl
from datetime import datetime
from minio_utils import get_minio_client, ensure_bucket

# -------------------------------
# Config
# -------------------------------
BUCKET_PROCESSED = "processed"
BUCKET_COMBINED = "combined"

class ProcessedCombiner:
    """Combines individual stock parquet files into one consolidated dataset."""

    def __init__(self):
        self.s3 = get_minio_client()
        ensure_bucket(self.s3, BUCKET_COMBINED)

    def list_parquet_files(self):
        """List all parquet files in processed/ bucket."""
        objects = self.s3.list_objects_v2(Bucket=BUCKET_PROCESSED).get("Contents", [])
        parquet_files = [obj["Key"] for obj in objects if obj["Key"].endswith(".parquet")]
        if not parquet_files:
            raise Exception("❌ No parquet files found in 'processed/' bucket.")
        return parquet_files

    def read_parquet_from_minio(self, key):
        """Read parquet file from MinIO into Polars DataFrame."""
        print(f"📂 Loading {key}")
        obj = self.s3.get_object(Bucket=BUCKET_PROCESSED, Key=key)
        return pl.read_parquet(io.BytesIO(obj["Body"].read()))

    def combine_all(self):
        """Combine all processed parquet files into one DataFrame."""
        parquet_files = self.list_parquet_files()
        frames = [self.read_parquet_from_minio(key) for key in parquet_files]
        combined_df = pl.concat(frames)
        print(f"✅ Combined {len(frames)} files successfully.")
        return combined_df

    def save_to_minio(self, df):
        """Save combined DataFrame as a new parquet file."""
        buffer = io.BytesIO()
        df.write_parquet(buffer)
        timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
        key = f"combined_stock_{timestamp}.parquet"
        self.s3.put_object(Bucket=BUCKET_COMBINED, Key=key, Body=buffer.getvalue())
        print(f"✅ Uploaded combined parquet → {key}")

    def run(self):
        """Execute full combine workflow."""
        combined_df = self.combine_all()
        self.save_to_minio(combined_df)
        print("🎯 Combined dataset ready in 'combined/' bucket.")


# -------------------------------
# Entry Point
# -------------------------------
if __name__ == "__main__":
    combiner = ProcessedCombiner()
    combiner.run()
