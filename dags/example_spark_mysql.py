import base64
import json
from datetime import timedelta

from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.apache.livy.operators.livy import LivyOperator
from airflow.providers.apache.livy.sensors.livy import LivySensor
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, BaseHook
from pendulum import datetime
from common import mmix_slack_operator as slack_operator

_slack_conn_id = Variable.get("mmix-slack-conn-id")
_slack_channel_id = Variable.get("mmix-slack-channel-id")
_aws_conn_id = Variable.get("mmix-aws-conn-id")
_environment = Variable.get("mmix-environment")
_livy_conn_id = Variable.get("mmix-livy-server-conn-id")
_s3_bucket_name = Variable.get("mmix-aws-s3-workreduce-bucket-name")
_mysql_conn = BaseHook.get_connection(Variable.get("mmix-mysql-primary-observability-conn-id"))
_mysql_json = json.dumps({"host": _mysql_conn.host, "port": _mysql_conn.port, "user": _mysql_conn.login, "password": _mysql_conn.password, "database": _mysql_conn.schema}, separators=(",", ":"))


@dag(dag_id="example_spark_mysql",
     default_args={
         "depends_on_past": False,
         "retries": None,
         "retry_delay": timedelta(seconds=5),
     },
     start_date=datetime(2026, 1, 1),
     schedule="*/30 * * * *",
     catchup=False,
     on_success_callback=slack_operator.build_dag_success_callback(_slack_conn_id, _slack_channel_id),
     on_failure_callback=slack_operator.build_dag_failure_callback(_slack_conn_id, _slack_channel_id),
     tags=["MMIX", "Example", "Spark", "Mysql"])
def example_spark_mysql():
    hook = AwsBaseHook(aws_conn_id=_aws_conn_id)
    connection = hook.get_connection(_aws_conn_id)
    session = hook.get_session()
    start_task = EmptyOperator(task_id="start_empty")
    end_task = EmptyOperator(task_id="end_empty")
    submit_spark_task = LivyOperator(
        task_id="submit_example_spark_mysql_job",
        livy_conn_id=_livy_conn_id,
        file=f"s3a://{_s3_bucket_name}/src/example-spark-mysql.py",
        pyFiles=[
            f"s3a://{_s3_bucket_name}/dist/enjoy_workreduce-0.0.1-py3-none-any.whl"
        ],
        name="airflow-livy-annotation",
        args=[
            "--dag_id", "{{ dag.dag_id }}",
            "--run_id", "{{ run_id }}",
            "--secret", base64.b64encode(_mysql_json.encode("utf-8")).decode("ascii"),
            "--logical_datetime", "{{logical_date.in_timezone('UTC').strftime('%Y-%m-%d %H:%M:%S')}}",
            "--environment", _environment,
        ],
        conf={
            "spark.executor.cores": "1",
            "spark.executor.memory": "1g",
            "spark.driver.memory": "1g",
            "spark.hadoop.fs.s3a.endpoint": connection.extra_dejson.get("s3_endpoint_url"),
            "spark.hadoop.fs.s3a.endpoint.region": connection.extra_dejson.get("region"),
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.access.key": session.get_credentials().access_key,
            "spark.hadoop.fs.s3a.secret.key": session.get_credentials().secret_key,
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.sql.shuffle.partitions": "100",
        },
        driver_memory="2g",
        executor_memory="4g",
        executor_cores=2,
        num_executors=3,
        polling_interval=10,
    )

    wait_spark_task = LivySensor(
        task_id="wait_example_spark_mysql_job",
        livy_conn_id=_livy_conn_id,
        batch_id="{{ ti.xcom_pull(task_ids='submit_example_spark_mysql_job') }}",
        poke_interval=15,
        timeout=60 * 60,
        mode="reschedule",
    )

    start_task >> submit_spark_task >> wait_spark_task >> end_task


example_spark_mysql()
