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


@dag(dag_id="example_dbt_postgresql",
     default_args={
         "start_date": datetime(2026, 1, 1),
         "retries": 0,
         "retry_delay": timedelta(seconds=10),
     },
     schedule=None,
     catchup=False,
     on_success_callback=slack_operator.build_dag_success_callback(Variable.get("mmix-slack-conn-id"), Variable.get("mmix-slack-channel-id")),
     on_failure_callback=slack_operator.build_dag_failure_callback(Variable.get("mmix-slack-conn-id"), Variable.get("mmix-slack-channel-id")),
     description="",
     tags=["DBT", "Postgresql", "MMIX", "Example"])
def example_dbt_postgresql():
    @task
    def dbt_version(ssh_conn_id: str) -> str:
        cmd = r"dbt --version"
        return run_remote_cmd(ssh_conn_id, cmd, timeout=60 * 60)

    @task
    def dbt_debug(ssh_conn_id: str) -> str:
        cmd = r"dbt debug --project-dir /home/dbt/projects/mmix --profiles-dir /home/dbt/.dbt --target dev"
        return run_remote_cmd(ssh_conn_id, cmd, timeout=60 * 60)

    @task
    def dbt_run(ssh_conn_id: str) -> str:
        cmd = r"dbt run --project-dir /home/dbt/projects/mmix --profiles-dir /home/dbt/.dbt --target dev"
        return run_remote_cmd(ssh_conn_id, cmd, timeout=60 * 60)

    @task
    def dbt_test(ssh_conn_id: str) -> str:
        cmd = r"dbt test --project-dir /home/dbt/projects/mmix --profiles-dir /home/dbt/.dbt --target dev"
        return run_remote_cmd(ssh_conn_id, cmd, timeout=60 * 60)

    start_task = EmptyOperator(task_id="start_empty")
    end_task = EmptyOperator(task_id="end_empty")

    (start_task >> dbt_version(_ssh_conn_id) >> dbt_debug(_ssh_conn_id) >> dbt_run(_ssh_conn_id) >> dbt_test(_ssh_conn_id) >> end_task)


example_dbt_postgresql()
