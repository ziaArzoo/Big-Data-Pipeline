from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys, os

# Add scripts folder to path (inside container)
sys.path.append("/opt/airflow/project/scripts")

from stock_ingestor import fetch_and_upload
from etl_transform import transform_and_upload
from combine_processed import ProcessedCombiner
from stock_predictor import StockPredictor  # we'll create next

default_args = {
    "owner": "Zia",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="stock_data_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 11, 5),
    schedule_interval="@daily",  # Run daily
    catchup=False,
    description="End-to-end stock data pipeline with ML prediction",
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_data",
        python_callable=fetch_and_upload
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_and_upload
    )

    combine_task = PythonOperator(
        task_id="combine_data",
        python_callable=lambda: ProcessedCombiner().run()
    )

    predict_task = PythonOperator(
        task_id="predict_future",
        python_callable=lambda: StockPredictor().run()
    )

    # DAG dependency chain
    ingest_task >> transform_task >> combine_task >> predict_task

