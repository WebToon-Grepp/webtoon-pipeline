from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

DBT_PROJECT_DIR = '/opt/airflow/analytics'

with DAG(
    dag_id="execute_dbt_analytics",
    schedule_interval="0 11 * * *", # 한국 시간 20시
    start_date=datetime(2025, 2, 26), 
    catchup=False, 
    tags=["analytics", "dbt"],
) as dag:
    
    run_dbt_model_task = BashOperator(
        task_id="run_dbt_model",
        bash_command=f"""
            cd {DBT_PROJECT_DIR} &&
            dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}
        """
    )

    run_dbt_model_task
