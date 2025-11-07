import io
import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from minio_utils import get_minio_client, ensure_bucket

# Config

PREDICTION_BUCKET = "predictions"
COMBINED_BUCKET = "combined"


class StockPredictor:
    def __init__(self):
        self.s3 = get_minio_client()
        ensure_bucket(self.s3, PREDICTION_BUCKET)

    # Get Latest Combined File
     def get_latest_combined(self):
        objs = self.s3.list_objects_v2(Bucket=COMBINED_BUCKET).get("Contents", [])
        if not objs:
            raise Exception("No combined dataset found!")

        latest = max(objs, key=lambda x: x["LastModified"])
        key = latest["Key"]
        print(f"Using latest combined file: {key}")

        obj = self.s3.get_object(Bucket=COMBINED_BUCKET, Key=key)
        df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
        print(f"Columns detected: {list(df.columns)}")
        return df

    # -------------------------------
    # Train & Predict
    # -------------------------------
    def train_and_predict(self, df):
        print("Training Linear Regression model...")

        # Normalize column names
        df.columns = [c.lower() for c in df.columns]
        symbol_col = next((c for c in df.columns if "symbol" in c), None)
        dt_col = next((c for c in df.columns if "datetime" in c), None)
        if not symbol_col or not dt_col:
            raise Exception(" Missing required symbol/datetime columns.")

        results = []

        for symbol in df[symbol_col].unique():
            group = df[df[symbol_col] == symbol].copy().sort_values(dt_col)

            # Detect columns dynamically
            def find_col(base):
                for c in df.columns:
                    if base in c:
                        return c
                return None

            close_col = find_col("close")
            open_col = find_col("open")
            high_col = find_col("high")
            low_col = find_col("low")
            vol_col = find_col("volume")

            features = [c for c in [open_col, high_col, low_col, vol_col] if c]

            if not close_col or not features:
                print(f"Skipping {symbol}: missing key columns.")
                continue

            # Create target and drop rows with NaNs
            group["target"] = group[close_col].shift(-1)
            group = group.dropna(subset=features + ["target"])
            if len(group) < 2:
                print(f" Skipping {symbol}: not enough data ({len(group)} rows).")
                continue

            X = group[features]
            y = group["target"]

            # Train model
            model = LinearRegression().fit(X, y)
            preds = model.predict(X)
            mse = mean_squared_error(y, preds)

            last_row = group.iloc[-1]
            next_pred = model.predict([[last_row[f] for f in features]])[0]

            results.append({
                "symbol": symbol,
                "predicted_close": round(next_pred, 2),
                "last_date": str(last_row[dt_col]),
                "mse": round(mse, 4)
            })

            print(f"{symbol}: Predicted next close = {next_pred:.2f} | MSE={mse:.4f}")

        if not results:
            raise Exception("No valid predictions generated (not enough rows).")

        return pd.DataFrame(results)

    # -------------------------------
    # Upload Results
    # -------------------------------
    def upload_predictions(self, df):
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        key = f"predictions_{datetime.now().strftime('%Y%m%dT%H%M%S')}.parquet"
        self.s3.put_object(Bucket=PREDICTION_BUCKET, Key=key, Body=buffer.getvalue())
        print(f" Uploaded predictions to '{key}'")

    def run(self):
        df = self.get_latest_combined()
        results = self.train_and_predict(df)
        self.upload_predictions(results)
        print(" ML predictions completed successfully!")


if __name__ == "__main__":
    StockPredictor().run()
