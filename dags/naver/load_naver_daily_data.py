import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.abspath("/opt/airflow"))

from airflow import DAG
from airflow.operators.python import PythonOperator
from plugins.dbshell_hook import DBShellHook
from plugins.slack_callback import dag_success_alert, task_failure_alert

BUCKET = "wt-grepp-lake"

def add_partition(target, **kwargs):
    execution_date = datetime.now()
    year = execution_date.year
    month = execution_date.month
    day = execution_date.day

    redshift_hook = DBShellHook(dbshell_conn_id="redshift_default")
    query = f"""
        ALTER TABLE external.{target}
        ADD PARTITION (year = {year}, month = {month:02d}, day = {day:02d}, platform = 'naver')
        LOCATION 's3://{BUCKET}/processed/{target}/year={year}/month={month:02d}/day={day:02d}/platform=naver/';
    """
    redshift_hook.execute_query(query=query)

with DAG(
    dag_id="load_naver_daily_data",
    schedule_interval=None, # process_naver_daily_data Trigger
    start_date=datetime(2025, 2, 26),
    catchup=False,
    on_success_callback=dag_success_alert,
    tags=["trigger", "daily", "redshift", "s3", "load", "naver"],
) as dag:
    
    add_titles_task = PythonOperator(
        task_id="add_partition_titles_table",
        python_callable=add_partition,
        op_args=["titles"],
        on_failure_callback=[task_failure_alert]
    )

    add_episodes_task = PythonOperator(
        task_id="add_partition_episodes_table",
        python_callable=add_partition,
        op_args=["episodes"],
        on_failure_callback=[task_failure_alert]
    )

    add_genres_task = PythonOperator(
        task_id="add_partition_genres_table",
        python_callable=add_partition,
        op_args=["genres"],
        on_failure_callback=[task_failure_alert]
    )

    [add_titles_task, add_episodes_task, add_genres_task]