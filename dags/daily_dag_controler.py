import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.abspath("/opt/airflow"))
sys.path.insert(0, os.path.abspath("/opt/airflow/crawler"))

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from plugins.slack_callback import dag_success_alert

from crawler.kakao import fetcher

with DAG(
    dag_id="daily_dag_controler",
    schedule_interval="@daily", # 한국 시간 9시
    start_date=datetime(2025, 2, 26),
    catchup=False,
    on_success_callback=dag_success_alert,
    tags=["daily", "controller", "dag"],
) as dag:

    with TaskGroup("trigger_fetch") as trigger_fetch:
        naver_fetch_task = TriggerDagRunOperator(
            task_id="trigger_naver_fetch",
            trigger_dag_id="fetch_and_store_naver_daily_data",
            wait_for_completion=True,
            poke_interval=60,
            deferrable=True
        )

        kakao_fetch_task = TriggerDagRunOperator(
            task_id="trigger_kakao_fetch",
            trigger_dag_id="fetch_and_store_kakao_daily_data",
            wait_for_completion=True,
            poke_interval=60,
            deferrable=True
        )

        [naver_fetch_task, kakao_fetch_task]

    with TaskGroup("trigger_optimize") as trigger_optimize:
        naver_optimize_task = TriggerDagRunOperator(
            task_id="trigger_naver_optimize",
            trigger_dag_id="optimize_kakao_daily_data",
            wait_for_completion=True,
            poke_interval=60,
            deferrable=True
        )

        kakao_optimize_task = TriggerDagRunOperator(
            task_id="trigger_kakao_optimize",
            trigger_dag_id="optimize_naver_daily_data",
            wait_for_completion=True,
            poke_interval=60,
            deferrable=True
        )

        naver_optimize_task >> kakao_optimize_task

    trigger_import_task = TriggerDagRunOperator(
        task_id="trigger_import",
        trigger_dag_id="import_external_tables",
        wait_for_completion=False
    )

    trigger_fetch >> trigger_optimize >> trigger_import_task
