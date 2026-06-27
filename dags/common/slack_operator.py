from airflow.providers.slack.operators.slack import SlackAPIPostOperator


def build_dag_success_callback(slack_conn_id: str, slack_channel: str):
    def _callback(context):
        SlackAPIPostOperator(
            task_id=f"slack_success_notify",
            slack_conn_id=slack_conn_id,
            username=f"Genie",
            text=f":white_check_mark: DAG *{context['dag'].dag_id}* 성공!\nRun ID: `{context['run_id']}`\nLog: {context['task_instance'].log_url}",
            channel=slack_channel,
        ).execute(context=context)

    return _callback


def build_dag_failure_callback(slack_conn_id: str, slack_channel: str):
    def _callback(context):
        SlackAPIPostOperator(
            task_id=f"slack_fail_notify",
            slack_conn_id=slack_conn_id,
            username=f"Genie",
            text=f"<!channel>:x: DAG *{context['dag'].dag_id}* 실패!\nRun ID: `{context['run_id']}`\nLog: {context['task_instance'].log_url}",
            channel=slack_channel,
        ).execute(context=context)

    return _callback


def build_dag_skipped_callback(slack_conn_id: str, slack_channel: str):
    def _callback(context):
        SlackAPIPostOperator(
            task_id=f"slack_skipped_notify",
            slack_conn_id=slack_conn_id,
            username=f"Genie",
            text=f"<!channel>:warning: DAG *{context['dag'].dag_id}* 스킵!\nRun ID: `{context['run_id']}`\nLog: {context['task_instance'].log_url}",
            channel=slack_channel,
        ).execute(context=context)

    return _callback
