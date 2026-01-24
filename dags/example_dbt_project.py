import json
import logging
from datetime import datetime
from datetime import timedelta

from airflow.models import Variable
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, task

from common import mmix_slack_operator as slack_operator

_ssh_conn_id = Variable.get("mmix-dbt-server-ssh-conn-id")


def run_remote_cmd(ssh_conn_id: str, command: str, timeout: int = 3600) -> str:
    hook = SSHHook(ssh_conn_id=ssh_conn_id)
    client = hook.get_conn()
    stdin, stdout, stderr = client.exec_command(command, timeout=timeout)
    out = stdout.read().decode("utf-8", errors="replace")
    err = stderr.read().decode("utf-8", errors="replace")
    exit_code = stdout.channel.recv_exit_status()
    if exit_code != 0:
        raise RuntimeError(f"Remote command failed (exit={exit_code})\nSTDERR:\n{err}\nSTDOUT:\n{out}")
    return out


@dag(dag_id="example_dbt_project",
     default_args={
         "start_date": datetime(2026, 1, 1),
         "retries": 0,
         "retry_delay": timedelta(seconds=10),
     },
     schedule="0 * * * *",
     catchup=False,
     on_success_callback=slack_operator.build_dag_success_callback(Variable.get("mmix-slack-conn-id"), Variable.get("mmix-slack-channel-id")),
     on_failure_callback=slack_operator.build_dag_failure_callback(Variable.get("mmix-slack-conn-id"), Variable.get("mmix-slack-channel-id")),
     description="",
     tags=["DBT", "Project", "MMIX", "External", "Example"])
def example_dbt_project():
    @task
    def event_daily(ssh_conn_id: str, **kwargs) -> str:
        logging.info(f"mmix_event_daily : {kwargs['logical_date']}")
        vars_json = json.dumps(json.dumps({"from_ts": f"{kwargs['logical_date']}"}))
        cmd = (
            f"bash -lc 'cd /home/dbt/projects/mmix && "
            f"dbt run --profiles-dir /home/dbt/.dbt "
            f"--target dev "
            f"--select event_daily "
            f"--vars {vars_json}'"
        )
        return run_remote_cmd(ssh_conn_id, cmd)

    @task
    def event_hourly(ssh_conn_id: str, **kwargs) -> str:
        logging.info(f"mmix_event_hourly : {kwargs['logical_date']}")
        vars_json = json.dumps(json.dumps({"from_ts": f"{kwargs['logical_date']}"}))
        cmd = (
            f"bash -lc 'cd /home/dbt/projects/mmix && "
            f"dbt run --profiles-dir /home/dbt/.dbt "
            f"--target dev "
            f"--select event_hourly "
            f"--vars {vars_json}'"
        )
        return run_remote_cmd(ssh_conn_id, cmd)

    start_task = EmptyOperator(task_id="start_empty")
    end_task = EmptyOperator(task_id="end_empty")

    (start_task >> event_daily(_ssh_conn_id) >> event_hourly(_ssh_conn_id) >> end_task)


example_dbt_project()
