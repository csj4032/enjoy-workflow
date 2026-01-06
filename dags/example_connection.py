import logging
from datetime import datetime

from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.sdk import dag, task

from common import mmix_slack_operator as slack_operator

_slack_conn_id = Variable.get("mmix-slack-conn-id")
_slack_channel_id = Variable.get("mmix-slack-channel-id")
_mysql_conn_id = Variable.get("mmix-aws-aurora-mysql-conn-id")
_aws_conn_id = Variable.get("mmix-aws-conn-id")
_bucket_name = Variable.get("mmix-aws-s3-workflow-bucket-name")
_prefix = Variable.get("mmix-aws-s3-workflow-bucket-prefix")


@dag(dag_id="example_connection",
     schedule_interval=None,
     catchup=False,
     start_date=datetime(2026, 1, 10),
     on_success_callback=slack_operator.build_dag_success_callback(_slack_conn_id, _slack_channel_id),
     on_failure_callback=slack_operator.build_dag_failure_callback(_slack_conn_id, _slack_channel_id),
     tags=["MMIX", "Connection", "Example"])
def example_connection():
    @task
    def connected_aws_mysql(mysql_conn_id: str) -> str:
        mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
        mysql_conn = mysql_hook.get_conn()
        cursor = mysql_conn.cursor()
        cursor.execute("SELECT DATABASE();")
        database_name = cursor.fetchone()
        logging.info(f"Connected to MySQL database: {database_name[0]}")
        return database_name

    @task()
    def connected_aws_s3(aws_conn_id: str, bucket_name: str, prefix: str = "") -> list:
        hook = S3Hook(aws_conn_id=aws_conn_id)
        keys = hook.list_keys(bucket_name=bucket_name, prefix=prefix)
        logging.info("keys: %s", keys)
        return keys

    connected_aws_mysql_task = connected_aws_mysql(_mysql_conn_id)
    connected_aws_s3_task = connected_aws_mysql(_aws_conn_id, _bucket_name, _prefix)
    connected_aws_mysql_task >> connected_aws_s3_task


example_connection()
