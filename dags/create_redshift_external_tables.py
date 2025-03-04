import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.abspath("/opt/airflow"))

from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.python import PythonOperator
from plugins.redshift_hook import RedshiftHook

DATABASE = "wt-grepp-spectrum"
BUCKET = "wt-grepp-lake"

def create_external_redshift_schema(**kwargs):
    iam_role = Variable.get("aws_iam_role")

    query = f"""
        CREATE EXTERNAL SCHEMA IF NOT EXISTS external
        FROM DATA CATALOG
        DATABASE '{DATABASE}'
        IAM_ROLE '{iam_role}'
        CREATE EXTERNAL DATABASE IF NOT EXISTS;
    """
    
    redshift_hook = RedshiftHook(query=query)
    redshift_hook.execute_query()

def create_external_titles_table(**kwargs):
    query = f"""
        CREATE EXTERNAL TABLE external.titles (
            id BIGINT,
            title VARCHAR(255),
            author VARCHAR(255),
            views BIGINT,
            image_url VARCHAR(1024),
            release_day INT,
            is_completed BOOLEAN
        )
        PARTITIONED BY (year INT, month INT, day INT, platform VARCHAR(255))
        STORED AS PARQUET
        LOCATION 's3://{BUCKET}/processed/titles/';
    """
    
    redshift_hook = RedshiftHook(query=query)
    redshift_hook.execute_query()

def create_external_episodes_table(**kwargs):
    query = f"""
        CREATE EXTERNAL TABLE external.episodes (
            title_id BIGINT,
            id BIGINT,
            title VARCHAR(255),
            likes BIGINT,
            comments BIGINT,
            image_url VARCHAR(1024),
            updated_date DATE
        )
        PARTITIONED BY (year INT, month INT, day INT, platform VARCHAR(255))
        STORED AS PARQUET
        LOCATION 's3://{BUCKET}/processed/episodes/';
    """
    
    redshift_hook = RedshiftHook(query=query)
    redshift_hook.execute_query()

def create_external_genres_table(**kwargs):
    query = f"""
        CREATE EXTERNAL TABLE external.genres (
            title_id BIGINT,
            genre_name VARCHAR(255)
        )
        PARTITIONED BY (year INT, month INT, day INT, platform VARCHAR(255))
        STORED AS PARQUET
        LOCATION 's3://{BUCKET}/processed/genres/';
    """
    
    redshift_hook = RedshiftHook(query=query)
    redshift_hook.execute_query()

with DAG(
    dag_id="create_redshift_external_tables",
    schedule_interval="@once",
    start_date=datetime(2025, 2, 26),
    catchup=False,
    tags=["create", "s3", "external", "redshift"],
) as dag:
    
    create_schema_task = PythonOperator(
        task_id="create_external_redshift_schema",
        python_callable=create_external_redshift_schema,
    )

    create_titles_task = PythonOperator(
        task_id="create_external_titles_table",
        python_callable=create_external_titles_table,
    )

    create_episodes_task = PythonOperator(
        task_id="create_external_episodes_table",
        python_callable=create_external_episodes_table,
    )

    create_genres_task = PythonOperator(
        task_id="create_external_genres_table",
        python_callable=create_external_genres_table,
    )

    create_schema_task >> [create_titles_task, create_episodes_task, create_genres_task]