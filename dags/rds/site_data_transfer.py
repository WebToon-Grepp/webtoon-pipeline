
import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.abspath("/opt/airflow"))

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.transfers.redshift_to_s3 import RedshiftToS3Operator
from plugins.slack_callback import dag_success_alert, task_failure_alert

with DAG(
    dag_id="site_data_transfer",
    schedule_interval=None, # site_data_transform Trigger
    start_date=datetime(2025, 2, 26),
    catchup=False,
    on_success_callback=dag_success_alert,
    tags=["trigger", "site", "transfer", "s3", "redshift"], 
) as dag:

    transfer_dim_webtoon_list_task = RedshiftToS3Operator(
        task_id="transfer_dim_webtoon_list",
        s3_bucket="wt-grepp-lake",
        s3_key="redshift/dim_webtoon_list",
        schema="analytics",
        table="dim_webtoon_list",
        include_header=True,
        unload_options=["ALLOWOVERWRITE", "DELIMITER ','", "ADDQUOTES", "PARALLEL OFF", ],
        on_failure_callback=[task_failure_alert]
    )

    transfer_dim_webtoon_genres_task = RedshiftToS3Operator(
        task_id="transfer_dim_webtoon_genres",
        s3_bucket="wt-grepp-lake",
        s3_key="redshift/dim_webtoon_genres",
        schema="analytics",
        table="dim_webtoon_genres",
        include_header=True,
        unload_options=["ALLOWOVERWRITE", "DELIMITER ','", "ADDQUOTES", "PARALLEL OFF"],
        on_failure_callback=[task_failure_alert]
    )

    transfer_fct_webtoon_episodes_task = RedshiftToS3Operator(
        task_id="transfer_fct_webtoon_episodes",
        s3_bucket="wt-grepp-lake",
        s3_key="redshift/fct_webtoon_episodes",
        schema="analytics",
        table="fct_webtoon_episodes",
        include_header=True,
        unload_options=["ALLOWOVERWRITE", "DELIMITER ','", "ADDQUOTES", "PARALLEL OFF"],
        on_failure_callback=[task_failure_alert]
    )

    trigger_copy_task = TriggerDagRunOperator(
        task_id="trigger_copy",
        trigger_dag_id="site_data_copy",
        wait_for_completion=False
    )

    [transfer_dim_webtoon_list_task, transfer_dim_webtoon_genres_task, transfer_fct_webtoon_episodes_task] >> trigger_copy_task