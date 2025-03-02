import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.abspath("/opt/airflow"))
sys.path.insert(0, os.path.abspath("/opt/airflow/crawler"))

from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from plugins.local_dir_to_s3 import LocalFoldersystemToS3Operator

from crawler.naver import fetcher

def fetch_data(**kwargs):
    execution_date = kwargs['execution_date']
    target_day = execution_date.weekday()
    
    fetcher.fetch_daily_data(target_day)

with DAG(
    dag_id="fetch_and_store_naver_daily_data",
    schedule_interval="0 23 * * *",
    start_date=datetime(2025, 2, 26),
    catchup=False,
    tags=["daily", "spark", "store", "s3", "fetch", "crawler", "naver"],
) as dag:
    
    fetch_data_task = PythonOperator(
        task_id="fetch_data",
        python_callable=fetch_data,
        dag=dag
    )

    upload_raw_data_to_s3_task = LocalFoldersystemToS3Operator(
        task_id="upload_raw_data_to_s3",
        folder="output/raw/naver", 
        folder_key="output",
        dest_bucket="wt-grepp-lake", 
        replace=True
    )

    transform_data_task = SparkSubmitOperator(
        task_id="transform_data",
        application="/opt/airflow/crawler/naver/processer.py", 
        packages="org.apache.hadoop:hadoop-aws:3.2.2",
        conf={
            "spark.hadoop.fs.s3a.access.key": Variable.get("aws_access_key"),
            "spark.hadoop.fs.s3a.secret.key": Variable.get("aws_secret_key"),
        }, 
        dag=dag
    )

    fetch_data_task >> upload_raw_data_to_s3_task >> transform_data_task
