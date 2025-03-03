import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.abspath("/opt/airflow"))

from airflow import DAG
from airflow.operators.python import PythonOperator
from plugins.redshift_hook import RedshiftHook

BUCKET = "wt-grepp-lake"

def add_partition(target, **kwargs):
    execution_date = datetime.now()
    year = execution_date.year
    month = execution_date.month
    day = execution_date.day

    redshift_hook = RedshiftHook()
    for platform in ['naver', 'kakao']:
        query = f"""
            ALTER TABLE external.{target}
            ADD PARTITION (year = {year}, month = {month:02d}, day = {day:02d}, platform = '{platform}')
            LOCATION 's3://{BUCKET}/processed/{target}/year={year}/month={month:02d}/day={day:02d}/platform={platform}/';
        """
        redshift_hook.execute_query(query=query)

with DAG(
    dag_id="add_partition_redshift_table",
    schedule_interval="30 10 * * *", # 한국 시간 19시 30분
    start_date=datetime(2025, 2, 26),
    catchup=False,
    tags=["partition", "s3", "external", "redshift"],
) as dag:
    add_titles_task = PythonOperator(
        task_id='add_partition_titles_table',
        python_callable=add_partition,
    )

    add_episodes_task = PythonOperator(
        task_id='add_partition_episodes_table',
        python_callable=add_partition,
    )

    add_genres_task = PythonOperator(
        task_id='add_partition_genres_table',
        python_callable=add_partition,
    )

    [add_titles_task, add_episodes_task, add_genres_task]