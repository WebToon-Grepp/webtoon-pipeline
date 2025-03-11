import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.abspath("/opt/airflow"))

from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from plugins.slack_callback import dag_success_alert, task_failure_alert

with DAG(
    dag_id="process_naver_daily_data",
    schedule_interval=None, # optimize_naver_daily_data Trigger
    start_date=datetime(2025, 2, 26),
    catchup=False,
    on_success_callback=dag_success_alert,
    tags=["trigger", "daily", "spark", "s3", "process", "naver"],
) as dag:
    
    processing_data_task = SparkSubmitOperator(
        task_id="transform_data",
        application="/opt/airflow/crawler/naver/processer.py", 
        packages="org.apache.hadoop:hadoop-aws:3.2.2",
        conf={
            "spark.hadoop.fs.s3a.access.key": Variable.get("aws_access_key"),
            "spark.hadoop.fs.s3a.secret.key": Variable.get("aws_secret_key"),
        },
        on_failure_callback=[task_failure_alert]
    )

    trigger_load_task = TriggerDagRunOperator(
        task_id="trigger_load",
        trigger_dag_id="load_naver_daily_data",
        wait_for_completion=True,
        poke_interval=30,
        deferrable=True
    )

    processing_data_task >> trigger_load_task
