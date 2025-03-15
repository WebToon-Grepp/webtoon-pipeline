import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.abspath("/opt/airflow"))

from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from plugins.slack_callback import dag_success_alert, task_failure_alert

DBT_PATH = '/home/airflow/.local/bin'
DBT_PROJECT_DIR = '/opt/airflow/analytics'

with DAG(
    dag_id="site_data_transform",
    schedule_interval=None, # import_external_tables Trigger
    start_date=datetime(2025, 2, 26), 
    catchup=False, 
    tags=["trigger", "site", "transform", "dbt", "redshift"], 
    on_success_callback=dag_success_alert
) as dag:
    
    run_dbt_model_task = BashOperator(
        task_id="run_dbt_model",
        env={
            "DBT_DBNAME": Variable.get("DBT_DBNAME"),
            "DBT_HOST": Variable.get("DBT_HOST"),
            "DBT_PASSWORD": Variable.get("DBT_PASSWORD"),
            "DBT_SCHEMA": Variable.get("DBT_SCHEMA"),
            "DBT_USER": Variable.get("DBT_USER")
        },
        bash_command=f"""
            cd {DBT_PROJECT_DIR} &&
            {DBT_PATH}/dbt run --profiles-dir {DBT_PROJECT_DIR} --target analytics --models site
        """,
        on_failure_callback=[task_failure_alert]
    )
    
    test_dbt_model_task = BashOperator(
        task_id="test_dbt_model",
        env={
            "DBT_DBNAME": Variable.get("DBT_DBNAME"),
            "DBT_HOST": Variable.get("DBT_HOST"),
            "DBT_PASSWORD": Variable.get("DBT_PASSWORD"),
            "DBT_SCHEMA": Variable.get("DBT_SCHEMA"),
            "DBT_USER": Variable.get("DBT_USER")
        },
        bash_command=f"""
            cd {DBT_PROJECT_DIR} &&
            {DBT_PATH}/dbt test --profiles-dir {DBT_PROJECT_DIR} --target analytics --models site
        """,
        on_failure_callback=[task_failure_alert]
    )
    
    trigger_transfer_task = TriggerDagRunOperator(
        task_id="trigger_transfer",
        trigger_dag_id="site_data_transfer",
        wait_for_completion=True,
        poke_interval=30,
        deferrable=True
    )

    run_dbt_model_task >> test_dbt_model_task >> trigger_transfer_task
