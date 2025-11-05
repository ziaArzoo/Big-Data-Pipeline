
# 🌩️ Cloud-Native Stock Data Pipeline

## 🎯 Objective
A modular, cloud-native data pipeline for real-time stock analytics — showcasing distributed ETL, S3 lake integration, and ML-driven forecasting.

---

## 🧱 Architecture

```
┌──────────────────────┐
│     yFinance API     │
└─────────┬────────────┘
          │
          ▼
 [stock_ingestor.py]
     ├─ Fetch JSON data
     └─ Upload to MinIO (raw/)

          ▼
 [transform.py]
     ├─ Clean, normalize
     └─ Save to MinIO (processed/)

          ▼
 [combine_processed.py]
     ├─ Merge per-symbol datasets
     └─ Save combined parquet

          ▼
 [stock_predictor.py]
     ├─ Train ML model (Linear Regression)
     ├─ Generate predictions
     └─ Upload to MinIO (predictions/)
```
## High Level
```
yFinance API → [stock_ingestor.py] → MinIO (raw/)
                     ↓
             [transform.py] → MinIO (processed/)
                     ↓
          [combine_processed.py] → MinIO (combined/)
                     ↓
           [stock_predictor.py] → MinIO (predictions/)
```
---

## ⚙️ Tech Stack
- **Language:** Python
- **ETL Orchestration:** Apache Airflow
- **Processing:** Polars
- **Storage:** MinIO (S3-compatible)
- **ML:** Scikit-learn (Linear Regression)
- **API Source:** yFinance
- **Containerization:** Docker
- **Version Control:** Git

---

## 📂 Project Structure
```
BigDataProject/
 ┣ scripts/
 ┃ ┣ stock_ingestor.py
 ┃ ┣ transform.py
 ┃ ┣ combine_processed.py
 ┃ ┣ stock_predictor.py
 ┃ ┗ minio_utils.py
 ┣ docker-compose.yml
 ┗ README.md
```

---

## 🚀 How to Run

### 1️⃣ Start Infrastructure
```bash
docker-compose up -d
```

### 2️⃣ Activate Virtual Environment
```bash
.env\Scriptsctivate
```

### 3️⃣ Run Pipeline
```bash
python -m scripts.stock_ingestor
python -m scripts.transform
python -m scripts.combine_processed
python -m scripts.stock_predictor
```

### 4️⃣ View Buckets
- `raw/` → Ingested JSON files  
- `processed/` → Cleaned parquet per stock  
- `combined/` → Merged dataset  
- `predictions/` → Model results  

---

