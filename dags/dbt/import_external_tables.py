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
    dag_id="import_external_tables",
    schedule_interval=None, # daily_dag_controller Trigger
    start_date=datetime(2025, 2, 26), 
    catchup=False, 
    tags=["trigger", "external", "internal", "import", "dbt", "redshift"], 
    on_success_callback=dag_success_alert
) as dag:
    
    run_dbt_import_task = BashOperator(
        task_id="run_dbt_import",
        env={
        },
        bash_command=f"""
            cd {DBT_PROJECT_DIR} &&
            {DBT_PATH}/dbt run --profiles-dir {DBT_PROJECT_DIR} --target raw_data --models import
        """,
        on_failure_callback=[task_failure_alert]
    )
    
    run_dbt_staging_task = BashOperator(
        task_id="run_dbt_staging",
        env={
        },
        bash_command=f"""
            cd {DBT_PROJECT_DIR} &&
            {DBT_PATH}/dbt run --profiles-dir {DBT_PROJECT_DIR} --target analytics --models staging
        """
    )

    trigger_dashboard_task = TriggerDagRunOperator(
        task_id="trigger_dashboard",
        trigger_dag_id="dashboard_data_transform",
        wait_for_completion=True,
        poke_interval=30,
        deferrable=True
    )

    trigger_site_task = TriggerDagRunOperator(
        task_id="trigger_site",
        trigger_dag_id="site_data_transform",
        wait_for_completion=True,
        poke_interval=30,
        deferrable=True
    )

    run_dbt_import_task >> run_dbt_staging_task >> [trigger_dashboard_task, trigger_site_task]