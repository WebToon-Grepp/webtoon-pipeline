import sys
import os
from datetime import datetime
import pendulum

sys.path.insert(0, os.path.abspath("/opt/airflow"))
sys.path.insert(0, os.path.abspath("/opt/airflow/crawler"))

from airflow import DAG
from airflow.operators.python import PythonOperator

from crawler.naver import fetcher

def run_daily_data(**kwargs):
    execution_date = kwargs['execution_date']
    target_day = execution_date.weekday()
    
    fetcher.fetch_daily_data(target_day)

with DAG(
    dag_id="fetch_and_store_naver_daily_data",
    schedule_interval="@once",
    start_date=datetime(2025, 2, 26),
    catchup=False,
    tags=["daily", "store", "s3", "fetch", "crawler", "naver"],
) as dag:
    
    run_daily_data_task = PythonOperator(
        task_id="run_daily_data",
        python_callable=run_daily_data,
        dag=dag
    )
    
    run_daily_data_task