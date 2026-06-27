import logging
from datetime import datetime
from datetime import timedelta

from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchSQLHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.sdk import dag, task, TriggerRule
from elastic_transport import ObjectApiResponse

from common import slack_operator


@dag(dag_id="connection",
     default_args={
         "depends_on_past": False,
         "retries": 1,
         "retry_delay": timedelta(seconds=5),
     },
     start_date=datetime(2026, 1, 1),
     schedule=None,
     catchup=False,
     on_success_callback=slack_operator.build_dag_success_callback(Variable.get("mmix-slack-conn-id"), Variable.get("mmix-slack-channel-id")),
     on_failure_callback=slack_operator.build_dag_failure_callback(Variable.get("mmix-slack-conn-id"), Variable.get("mmix-slack-channel-id")),
     tags=["MMIX", "Connection", "Example!!!!!!!!$#@$%@#$%$#@%$#%!!!!!"])
def example_connection():
    @task
    def connected_mysql_primary(mysql_conn_id: str) -> str:
        logging.info("mysql_conn_id: %s", mysql_conn_id)
        mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
        mysql_conn = mysql_hook.get_conn()
        cursor = mysql_conn.cursor()
        cursor.execute("SELECT DATABASE();")
        database_name = cursor.fetchone()
        logging.info(f"Connected to MySQL Database: {database_name[0]}")
        return database_name[0]

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def connected_mysql_replication(mysql_conn_id: str) -> str:
        logging.info("mysql_conn_id: %s", mysql_conn_id)
        mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
        mysql_conn = mysql_hook.get_conn()
        cursor = mysql_conn.cursor()
        cursor.execute("SELECT DATABASE();")
        database_name = cursor.fetchone()
        logging.info(f"Connected to MySQL Database: {database_name[0]}")
        return database_name[0]

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def connected_elasticsearch(elasticsearch_conn_id) -> ObjectApiResponse:
        logging.info("elasticsearch_conn_id: %s", elasticsearch_conn_id)
        elasticsearch = ElasticsearchSQLHook(elasticsearch_conn_id=elasticsearch_conn_id)
        elasticsearch_conn = elasticsearch.get_conn()
        info = elasticsearch_conn.es.info()
        logging.info(f"Connected to Elasticsearch: {info}")
        return info.body

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def connected_aws_s3(aws_conn_id: str, bucket_name: str, prefix: str = "") -> list:
        logging.info("aws_conn_id: %s, bucket_name: %s, prefix: %s", aws_conn_id, bucket_name, prefix)
        hook = S3Hook(aws_conn_id=aws_conn_id)
        keys = hook.list_keys(bucket_name=bucket_name, prefix=prefix)
        logging.info("Keys: %s", keys)
        return keys

    connected_primary_mysql_task = connected_mysql_primary(Variable.get("mmix-mysql-primary-mmix-conn-id"))
    connected_replication_mysql_task = connected_mysql_replication(Variable.get("mmix-mysql-replication-mmix-conn-id"))
    connected_elasticsearch_task = connected_elasticsearch(Variable.get("mmix-elasticsearch-conn-id"))
    connected_aws_s3_task = connected_aws_s3(Variable.get("mmix-aws-conn-id"), Variable.get("mmix-aws-s3-workflow-bucket-name"), Variable.get("mmix-aws-s3-workflow-bucket-prefix"))
    connected_primary_mysql_task >> connected_replication_mysql_task >> connected_elasticsearch_task >> connected_aws_s3_task


example_connection()
