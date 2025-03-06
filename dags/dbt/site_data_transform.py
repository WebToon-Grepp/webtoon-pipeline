from datetime import datetime

from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.bash import BashOperator

DBT_PATH = '/home/airflow/.local/bin'
DBT_PROJECT_DIR = '/opt/airflow/analytics'

with DAG(
    dag_id="site_data_transform",
    schedule_interval=None, # Trigger
    start_date=datetime(2025, 2, 26), 
    catchup=False, 
    tags=["trigger", "site", "transform", "dbt", "redshift"], 
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
            {DBT_PATH}/dbt run --profiles-dir {DBT_PROJECT_DIR} --target analytics --models staging site
        """
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
            {DBT_PATH}/dbt test --profiles-dir {DBT_PROJECT_DIR} --target analytics --models staging site
        """
    )

    run_dbt_model_task >> test_dbt_model_task
