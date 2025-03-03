import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.abspath("/opt/airflow"))
sys.path.insert(0, os.path.abspath("/opt/airflow/crawler"))

from airflow import DAG
from airflow.operators.python import PythonOperator
from plugins.local_dir_to_s3 import LocalFoldersystemToS3Operator

from crawler.kakao import fetcher

def fetch_data(**kwargs):
    fetcher.fetch_all_historical_data()

with DAG(
    dag_id="fetch_and_store_kakao_historical_data",
    schedule_interval="@once",
    start_date=datetime(2025, 2, 26),
    catchup=False,
    tags=["historical", "store", "s3", "fetch", "crawler", "kakao"],
) as dag:
    
    fetch_data_task = PythonOperator(
        task_id="fetch_data",
        python_callable=fetch_data,
        dag=dag
    )

    upload_raw_data_to_s3_task = LocalFoldersystemToS3Operator(
        task_id="upload_raw_data_to_s3",
        folder="output/raw/kakao", 
        folder_key="output",
        dest_bucket="wt-grepp-lake", 
        replace=True
    )
    
    fetch_data_task >> upload_raw_data_to_s3_task