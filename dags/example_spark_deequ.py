import base64
import json
import time
import uuid
from datetime import timedelta
from typing import Optional, Dict, Any

from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, task, BaseHook
from pendulum import datetime

from common import mmix_slack_operator as slack_operator

_livy_server_http_conn_id = Variable.get("mmix-livy-server-http-conn-id")
_mysql_conn = BaseHook.get_connection(Variable.get("mmix-mysql-primary-observability-conn-id"))
_mysql_json = json.dumps({"host": _mysql_conn.host, "port": _mysql_conn.port, "user": _mysql_conn.login, "password": _mysql_conn.password, "database": _mysql_conn.schema}, separators=(",", ":"))


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


@dag(dag_id="example_spark",
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
     tags=["MMIX", "Example", "Spark"])
def example_spark():
    @task
    def submit_batch(**kwargs) -> int:
        payload = {
            "name": f"{kwargs['dag'].dag_id}-{kwargs['run_id']}-try{kwargs['ti'].try_number}-{uuid.uuid4().hex[:8]}",
            "file": "s3a://mmix-prod-dataengineer-workreduce/src/example_deequ.py",
            "pyFiles": [
                "s3a://mmix-prod-dataengineer-workreduce/dist/enjoy_workreduce-0.0.1-py3-none-any.whl"
            ],
            "args": [
                "--dag_id", kwargs["dag"].dag_id,
                "--run_id", kwargs["run_id"],
                "--secret", base64.b64encode(_mysql_json.encode("utf-8")).decode("ascii"),
                "--logical_datetime", kwargs["logical_date"].in_timezone("UTC").strftime("%Y-%m-%d %H:%M:%S"),
                "--environment", Variable.get("mmix-environment"),
            ],
            "conf": {
                "spark.executor.cores": "1",
                "spark.executor.memory": "1g",
                "spark.driver.memory": "1g",
                "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
                "spark.hadoop.fs.s3a.path.style.access": "true",
                "spark.hadoop.fs.s3a.access.key": "mmix",
                "spark.hadoop.fs.s3a.secret.key": "mmixmmix",
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
    fetch_log_task = fetch_log(batch_id_task)
    start_task >> batch_id_task >> wait_for_batch_task >> fetch_log_task >> end_task


example_spark()
