from airflow import DAG
from airflow.operators.empty import EmptyOperator
from cosmos.providers.dbt.task_group import DbtTaskGroup
from datetime import datetime

from include.slack_webhook import dag_success_alert, task_failure_alert

DBT_EXECUTABLE_PATH = "/usr/local/bin/dbt"
DBT_ROOT_PATH = "/usr/local/airflow/dags/dbt"
PROJECT_NAME = "redshift"

with DAG(
    dag_id="dbt_daily_dag",
    start_date=datetime(2023, 6, 13),
    schedule="0 5 * * *",
    on_success_callback=dag_success_alert,
    on_failure_callback=task_failure_alert
):
    e1 = EmptyOperator(task_id="pre_dbt")

    dbt_tg = DbtTaskGroup(
        dbt_root_path=DBT_ROOT_PATH,
        dbt_project_name=PROJECT_NAME,
        conn_id="redshift_conn",
        operator_args={
            "project_dir": f"{DBT_ROOT_PATH}/{PROJECT_NAME}",
        },
        test_behavior="none",
        profile_args={
            "schema": "public"
        },
        select={"configs": ["tags:daily"]}
    )

    e2 = EmptyOperator(task_id="post_dbt")

    e1 >> dbt_tg >> e2