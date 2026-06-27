import base64
import json
import logging
import time
from datetime import timedelta
from typing import Optional, Dict, Any

from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, task
from pendulum import datetime

from common import slack_operator
from common import utils

_aws_conn_id = Variable.get("mmix-aws-conn-id")
_environment = Variable.get("mmix-environment")
_s3_bucket_name = Variable.get("mmix-aws-s3-workreduce-bucket-name")
_livy_server_http_conn_id = Variable.get("mmix-livy-server-http-conn-id")
_postgresql_observability_json = utils.build_postgresql_conn_json(Variable.get("mmix-postgresql-observability-conn-id"))


def _http_json(conn_id: str, method: str, endpoint: str, data: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None, extra_options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    hook = HttpHook(method=method, http_conn_id=conn_id)
    _headers = {"Content-Type": "application/json"}
    if headers:
        _headers.update(headers)

    body = json.dumps(data) if data is not None else None
    resp = hook.run(endpoint=endpoint, data=body, headers=_headers, extra_options=extra_options or {})

    if resp.status_code >= 400:
        raise AirflowException(f"Livy API error: {resp.status_code}, endpoint={endpoint}, body={resp.text}")

    text = (resp.text or "").strip()
    return json.loads(text) if text else {}


@dag(dag_id="spark_deequ",
     default_args={
         "depends_on_past": False,
         "retries": None,
         "retry_delay": timedelta(seconds=5),
     },
     start_date=datetime(2026, 1, 1),
     schedule="*/10 * * * *",
     catchup=False,
     on_success_callback=slack_operator.build_dag_success_callback(Variable.get("mmix-slack-conn-id"), Variable.get("mmix-slack-channel-id")),
     on_failure_callback=slack_operator.build_dag_failure_callback(Variable.get("mmix-slack-conn-id"), Variable.get("mmix-slack-channel-id")),
     description="An example DAG to run Spark job that performs data quality checks using Deequ.",
     tags=["MMIX", "Example", "Spark", "Deequ"])
def example_spark_deequ():
    @task
    def submit_batch(**kwargs) -> int:
        hook = AwsBaseHook(aws_conn_id=_aws_conn_id)
        connection = hook.get_connection(_aws_conn_id)
        session = hook.get_session()
        logging.info(f"Connecting to {connection.extra_dejson.get("s3_endpoint_url")}, {connection.extra_dejson.get("region")}")
        payload = {
            "name": f"{kwargs['dag'].dag_id}-{kwargs['run_id']}-try{kwargs['ti'].try_number}",
            "file": f"s3a://{_s3_bucket_name}/src/example_spark_deequ.py",
            "pyFiles": [
                f"s3a://{_s3_bucket_name}/dist/enjoy_workreduce-0.0.1-py3-none-any.whl"
            ],
            "args": [
                "--dag_id", kwargs["dag"].dag_id,
                "--run_id", kwargs["run_id"],
                "--secret", base64.b64encode(_postgresql_observability_json.encode("utf-8")).decode("ascii"),
                "--logical_datetime", kwargs["logical_date"].in_timezone("UTC").strftime("%Y-%m-%d %H:%M:%S"),
                "--environment", _environment,
            ],
            "conf": {
                "spark.executor.cores": "1",
                "spark.executor.memory": "1g",
                "spark.driver.memory": "1g",
                "spark.hadoop.fs.s3a.endpoint": connection.extra_dejson.get("endpoint_url"),
                "spark.hadoop.fs.s3a.endpoint.region": connection.extra_dejson.get("region_name"),
                "spark.hadoop.fs.s3a.path.style.access": "true",
                "spark.hadoop.fs.s3a.access.key": session.get_credentials().access_key,
                "spark.hadoop.fs.s3a.secret.key": session.get_credentials().secret_key,
                "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            }
        }
        res = _http_json(conn_id=_livy_server_http_conn_id, method="POST", endpoint="/batches", data=payload)
        batch_id_ = res.get("id")
        if batch_id_ is None:
            raise AirflowException(f"Livy did not return batch id. response={res}")

        return int(batch_id_)

    @task
    def wait_for_batch(batch_id_: int) -> str:
        terminal_success = {"success"}
        terminal_failure = {"dead", "killed", "error"}
        poll_interval_sec = 10
        timeout_sec = 60 * 30
        deadline = time.time() + timeout_sec

        while time.time() < deadline:
            res = _http_json(conn_id=_livy_server_http_conn_id, method="GET", endpoint=f"/batches/{batch_id_}/state")
            state = (res.get("state") or "").lower()

            if not state:
                detail = _http_json(conn_id=_livy_server_http_conn_id, method="GET", endpoint=f"/batches/{batch_id_}")
                state = (detail.get("state") or "").lower()

            if state in terminal_success:
                return "SUCCESS"

            if state in terminal_failure:
                log = _http_json(conn_id=_livy_server_http_conn_id, method="GET", endpoint=f"/batches/{batch_id_}/log?from=0&size=200")
                raise AirflowException(f"Livy batch failed. batch_id={batch_id_}, state={state}, log={log}")

            time.sleep(poll_interval_sec)

        raise AirflowException(f"Timeout waiting for Livy batch. batch_id={batch_id_}")

    @task
    def fetch_log(batch_id_: int) -> None:
        log = _http_json(conn_id=_livy_server_http_conn_id, method="GET", endpoint=f"/batches/{batch_id_}/log?from=0&size=200")
        print(f"[Livy batch log] batch_id={batch_id_}\n{json.dumps(log, indent=2)}")

    start_task = EmptyOperator(task_id="start_empty")
    end_task = EmptyOperator(task_id="end_empty")
    batch_id_task = submit_batch()
    wait_for_batch_task = wait_for_batch(batch_id_task)
    start_task >> batch_id_task >> wait_for_batch_task >> fetch_log(batch_id_task) >> end_task


example_spark_deequ()
