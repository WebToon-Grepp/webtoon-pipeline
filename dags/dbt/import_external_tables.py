import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.abspath("/opt/airflow"))

from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from plugins.slack_callback import dag_success_alert, task_failure_alert

DBT_PATH = '/home/airflow/.local/bin'
DBT_PROJECT_DIR = '/opt/airflow/analytics'

with DAG(
    dag_id="import_external_tables",
    schedule_interval="@daily", # 한국 시간 9시
    start_date=datetime(2025, 2, 26), 
    catchup=False, 
    tags=["trigger", "external", "internal", "import", "dbt", "redshift"], 
    on_success_callback=dag_success_alert
) as dag:
    
    wait_for_naver_sensor = ExternalTaskSensor(
        task_id="wait_for_naver",
        external_dag_id="fetch_and_store_naver_daily_data",
        external_task_id="trigger_optimize",
        mode="reschedule",
        timeout=2500,
        poke_interval=500,
        allowed_states=["success"],
        failed_states=["failed", "skipped"]
    )

    wait_for_kakao_sensor = ExternalTaskSensor(
        task_id="wait_for_kakao",
        external_dag_id="fetch_and_store_kakao_daily_data",
        external_task_id="trigger_optimize",
        mode="reschedule",
        timeout=2500,
        poke_interval=500,
        allowed_states=["success"],
        failed_states=["failed", "skipped"]
    )
    
    run_dbt_import_task = BashOperator(
        task_id="run_dbt_import",
        env={
            "DBT_DBNAME": Variable.get("DBT_DBNAME"),
            "DBT_HOST": Variable.get("DBT_HOST"),
            "DBT_PASSWORD": Variable.get("DBT_PASSWORD"),
            "DBT_SCHEMA": Variable.get("DBT_SCHEMA"),
            "DBT_USER": Variable.get("DBT_USER")
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
            "DBT_DBNAME": Variable.get("DBT_DBNAME"),
            "DBT_HOST": Variable.get("DBT_HOST"),
            "DBT_PASSWORD": Variable.get("DBT_PASSWORD"),
            "DBT_SCHEMA": Variable.get("DBT_SCHEMA"),
            "DBT_USER": Variable.get("DBT_USER")
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
        poke_interval=100,
        deferrable=True
    )

    trigger_site_task = TriggerDagRunOperator(
        task_id="trigger_site",
        trigger_dag_id="site_data_transform",
        wait_for_completion=True,
        poke_interval=100,
        deferrable=True
    )

    [wait_for_naver_sensor, wait_for_kakao_sensor] >> run_dbt_import_task >> run_dbt_staging_task >> [trigger_dashboard_task, trigger_site_task]