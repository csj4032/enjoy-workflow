import json

from airflow.models import Variable
from airflow.sdk import BaseHook


def build_mysql_conn_json(conn_id_variable_name: str) -> str:
    conn_id = Variable.get(conn_id_variable_name)
    conn = BaseHook.get_connection(conn_id)
    payload = {"host": conn.host, "port": conn.port, "user": conn.login, "password": conn.password, "database": conn.schema, "driver": conn.extra_dejson.get("driver", "com.mysql.cj.jdbc.Driver")}
    return json.dumps(payload, separators=(",", ":"))
