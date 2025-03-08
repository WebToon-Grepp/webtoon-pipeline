
import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.abspath("/opt/airflow"))

from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from plugins.rds_hook import RDSHook
from plugins.slack_callback import dag_success_alert, task_failure_alert

def download_file_from_s3(key, column, copy_col):
    s3_hook = S3Hook(aws_conn_id="aws_default")
    tmp_file = s3_hook.download_file(
        key=f"redshift/{key}/{key}_000",
        bucket_name="wt-grepp-lake",
        local_path="/opt/airflow/tmp"
    )

    rds_hook = RDSHook()
    query = f" DROP TABLE IF EXISTS site.{key};"
    rds_hook.execute_query(query)

    query = f"CREATE TABLE site.{key} ({column});"
    rds_hook.execute_query(query)

    rds_hook.copy_file(f"site.{key}", tmp_file, copy_col)
    
with DAG(
    dag_id="site_data_copy",
    schedule_interval=None, # site_data_transfer Trigger
    start_date=datetime(2025, 2, 26),
    catchup=False,
    on_success_callback=dag_success_alert,
    tags=["trigger", "site", "copy", "s3", "redshift"], 
) as dag:
    
    clear_tmp_dir_task  = BashOperator(
        task_id='clear_tmp_dir',
        bash_command="rm -rf /opt/airflow/tmp && mkdir /opt/airflow/tmp",
    )

    copy_dim_webtoon_list_task = PythonOperator(
        task_id="copy_dim_webtoon_list",
        python_callable=download_file_from_s3,
        op_args=["dim_webtoon_list", 
                 """
                    platform CHARACTER VARYING,
                    id BIGINT,
                    title CHARACTER VARYING,
                    author CHARACTER VARYING,
                    image_url CHARACTER VARYING,
                    views DOUBLE PRECISION,
                    likes DOUBLE PRECISION,
                    comments DOUBLE PRECISION,
                    release_day INTEGER,
                    is_completed BOOLEAN
                """, 
                "platform, id, title, author, image_url, views, likes, comments, release_day, is_completed"
        ],
        on_failure_callback=[task_failure_alert]
    )

    copy_dim_webtoon_genres_task = PythonOperator(
        task_id="copy_dim_webtoon_genres",
        python_callable=download_file_from_s3,
        op_args=["dim_webtoon_genres", 
                 """
                    platform CHARACTER VARYING,
                    id BIGINT,
                    title CHARACTER VARYING,
                    author CHARACTER VARYING,
                    image_url CHARACTER VARYING,
                    views DOUBLE PRECISION,
                    likes DOUBLE PRECISION,
                    comments DOUBLE PRECISION,
                    genre_name CHARACTER VARYING
                """,
                "platform, id, title, author, image_url, views, likes, comments, genre_name"
        ],
        on_failure_callback=[task_failure_alert]
    )

    copy_fct_webtoon_episodes_task = PythonOperator(
        task_id="copy_fct_webtoon_episodes",
        python_callable=download_file_from_s3,
        op_args=["fct_webtoon_episodes", 
                 """
                    platform CHARACTER VARYING,
                    title_id BIGINT,
                    id BIGINT,
                    title CHARACTER VARYING,
                    likes DOUBLE PRECISION,
                    comments DOUBLE PRECISION,
                    image_url CHARACTER VARYING,
                    updated_date DATE
                """,
                "platform, title_id, id, title, likes, comments, image_url, updated_date"
        ],
        on_failure_callback=[task_failure_alert]
    )

    clear_tmp_dir_task >> [copy_dim_webtoon_list_task, copy_dim_webtoon_genres_task, copy_fct_webtoon_episodes_task]