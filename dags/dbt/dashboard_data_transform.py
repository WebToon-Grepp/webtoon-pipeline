import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.abspath("/opt/airflow"))

from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.bash import BashOperator
from plugins.slack_callback import dag_success_alert, task_failure_alert

DBT_PATH = '/home/airflow/.local/bin'
DBT_PROJECT_DIR = '/opt/airflow/analytics'

with DAG(
    dag_id="dashboard_data_transform",
    schedule_interval=None, # import_external_tables Trigger
    start_date=datetime(2025, 2, 26), 
    catchup=False, 
    tags=["trigger", "dashboard", "transform", "dbt", "redshift"], 
    on_success_callback=dag_success_alert
) as dag:
    
    run_dbt_model_task = BashOperator(
        task_id="run_dbt_model",
        env={
        },
        bash_command=f"""
            cd {DBT_PROJECT_DIR} &&
            {DBT_PATH}/dbt run --profiles-dir {DBT_PROJECT_DIR} --target analytics --models dashboard
        """,
        on_failure_callback=[task_failure_alert]
    )

    run_dbt_model_task
