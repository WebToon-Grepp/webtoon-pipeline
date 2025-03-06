from datetime import datetime

from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

DBT_PATH = '/home/airflow/.local/bin'
DBT_PROJECT_DIR = '/opt/airflow/analytics'

with DAG(
    dag_id="import_external_tables",
    schedule_interval="0 3 * * *", # 한국 시간 12시
    start_date=datetime(2025, 2, 26), 
    catchup=False, 
    tags=["trigger", "external", "internal", "import", "dbt", "redshift"], 
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
            {DBT_PATH}/dbt run --profiles-dir {DBT_PROJECT_DIR} --target raw_data --models import
        """
    )

    # 추후 진행 예정
    # trigger_dashboard_task = TriggerDagRunOperator(
    #     task_id="trigger_dashboard",
    #     trigger_dag_id="dashboard_data_transform",
    #     wait_for_completion=False
    # )

    trigger_site_task = TriggerDagRunOperator(
        task_id="trigger_site",
        trigger_dag_id="site_data_transform",
        wait_for_completion=False
    )

    run_dbt_model_task >> trigger_site_task #>> [trigger_dashboard_task, trigger_site_task]
