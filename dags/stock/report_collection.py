from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="stock__report_collection",
    schedule="0 9 * * 1-5",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["korea_stock"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    start >> end
