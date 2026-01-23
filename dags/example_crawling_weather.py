import io
import logging
from datetime import datetime
from datetime import timedelta

import numpy as np
from airflow.providers.mysql.hooks.mysql import MySqlHook
import great_expectations as gx
import pandas as pd
import requests
from airflow.models import Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, task, TaskGroup, TriggerRule

_slack_conn_id = Variable.get("mmix-slack-conn-id")
_slack_channel_id = Variable.get("mmix-slack-channel-id")
_mysql_conn_id = Variable.get("mmix-mysql-primary-external-conn-id")
_kma_auth_key = Variable.get("mmix-kma-auth-key")

_request_map = {
    "kma_sfctm2": {
        "description": "지상관측 종관기상관측 지상관측자료 조회 시간자료",
        "url_template": "https://apihub.kma.go.kr/api/typ01/url/kma_sfctm2.php?tm={tm}&stn={stn}&help={help}&authKey={authKey}",
        "columns": ['TM', 'STN', 'WD', 'WS', 'GST_WD', 'GST_WS', 'GST_TM', 'PA', 'PS', 'PT', 'PR', 'TA', 'TD', 'HM', 'PV', 'RN', 'RN_DAY', 'RN_JUN', 'RN_INT',
                    'SD_HR3', 'SD_DAY', 'SD_TOT', 'WC', 'WP', 'WW', 'CA_TOT', 'CA_MID', 'CH_MIN', 'CT', 'CT_TOP', 'CT_MID', 'CT_LOW', 'VS', 'SS', 'SI', 'ST_GD',
                    'TS', 'TE_005', 'TE_01', 'TE_02', 'TE_03', 'ST_SEA', 'WH', 'BF', 'IR', 'IX'],
        "database": "external",
        "table": "weather_hourly",
    },
    "nph_aws2_min": {
        "description": "지상관측 종관기상관측 AWS 매분자료 조회 AWS 매분자료",
        "url_template": "https://apihub.kma.go.kr/api/typ01/cgi-bin/url/nph-aws2_min?tm2={tm2}&stn={stn}&disp={disp}&help={help}&authKey={authKey}",
        "columns": ["TM", "STN", "WD1", "WS1", "WDS", "WSS", "WD10", "WS10", "TA", "RE", "RN_15m", "RN_60m", "RN_12H", "RN_DAY", "HM", "PA", "PS", "TD"],
        "database": "external",
        "table": "weather_minute"
    },
}


def get_api_url(request: dict, param: dict) -> str:
    return request["url_template"].format(**param)


def get_columns(key: str) -> list:
    return _request_map[key]["columns"]


def logical_date_to(logical_date, format_: str = "%Y%m%d%H%M", timedelta_: int = 9) -> str:
    logical_date_kst = logical_date + timedelta(hours=timedelta_)
    return logical_date_kst.strftime(format_)


def get_table_name(key: str) -> str:
    bq_info = _request_map[key]
    return f"{bq_info['bq_project']}.{bq_info['bq_dataset']}.{bq_info['bq_table']}"


def fetch_data(api_url: str) -> str:
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from API: {e}")
        return None


def remove_comment_lines(raw_text: str) -> str:
    if not raw_text:
        return ""
    lines = raw_text.splitlines()
    data_lines = [line for line in lines if not line.strip().startswith('#')]
    return "\n".join(data_lines)


def parse_data_to_dataframe(cleaned_text: str, column_names: list) -> pd.DataFrame:
    if not cleaned_text:
        return pd.DataFrame()
    data_io = io.StringIO(cleaned_text)
    dataframe = pd.read_csv(data_io, delim_whitespace=True, header=None, names=column_names)
    dataframe["TM"] = pd.to_datetime(dataframe["TM"], format="%Y%m%d%H%M").dt.tz_localize("Asia/Seoul")
    return dataframe


def _to_mysql_value(x):
    if x is None:
        return None
    if isinstance(x, float) and np.isnan(x):
        return None
    if pd.isna(x):
        return None
    if isinstance(x, (pd.Timestamp,)):
        if x.tzinfo is not None:
            x = x.tz_convert("Asia/Seoul").tz_localize(None)
        return x.strftime("%Y-%m-%d %H:%M:%S")
    if hasattr(x, "item"):
        try:
            return x.item()
        except Exception:
            pass
    return x


def load_to_mysql(mysql_conn_id: str, dataframe: pd.DataFrame, database: str, table: str, upsert: bool = True) -> None:
    if len(dataframe) == 0:
        logging.warning("No data to load into MySQL")
        return
    columns = list(dataframe.columns)
    records = [tuple(_to_mysql_value(v) for v in row) for row in dataframe.itertuples(index=False, name=None)]
    sql = f"INSERT INTO `{database}`.`{table}` ({", ".join([f"`{c}`" for c in columns])}) VALUES ({", ".join(["%s"] * len(columns))})"
    if upsert:
        update_cols = [c for c in columns if c not in ("tm", "stn")]
        update_sql = ", ".join([f"`{c}` = VALUES(`{c}`)" for c in update_cols])
        sql += f" ON DUPLICATE KEY UPDATE {update_sql}"
    logging.info(f"sql: {sql}")
    hook = MySqlHook(mysql_conn_id=mysql_conn_id)
    connection = hook.get_conn()
    try:
        with connection.cursor() as cursor:
            cursor.executemany(sql, records)
        connection.commit()
        logging.info("Inserted/Upserted %d rows into %s.%s", len(records), database, table)
    except Exception as e:
        connection.rollback()
        logging.exception("Failed to load data into MySQL: %s", e)
        raise
    finally:
        connection.close()


@dag(dag_id="example_crawling_weather",
     default_args={
         "start_date": datetime(2026, 1, 1),
         "retries": 0,
         "retry_delay": timedelta(seconds=10),
     },
     schedule="*/10 * * * *",
     catchup=False,
     description="An Airflow DAG that loads data from the KMA Hub API into Mysql",
     tags=["Weather", "Crawling", "Slack", "MMIX", "Example"])
def example_crawling_weather():
    start_task = EmptyOperator(task_id="start_task")
    end_task = EmptyOperator(task_id="end_task")

    @task.branch
    def branch_hourly(logical_date=None, **kwargs) -> str:
        if logical_date.minute == 0:
            return "kma_sfctm2_group.kma_sfctm2"
        else:
            return "skip_kma_sfctm2"

    @task(task_id="skip_kma_sfctm2")
    def skip_kma_sfctm2() -> None:
        logging.info("지상관측 종관기상관측 지상관측자료 조회 스킵")

    with TaskGroup("kma_sfctm2_group") as kma_sfctm2_group:
        @task(task_id="kma_sfctm2")
        def kma_sfctm2(mysql_conn_id: str, kma_auth_key: str, **kwargs) -> pd.DataFrame:
            logging.info("지상관측 종관기상관측 지상관측자료 조회")
            request = _request_map["kma_sfctm2"]
            tm = logical_date_to(kwargs['logical_date'])
            api_url = get_api_url(request, param={"tm": tm, "stn": "0", "help": "0", "authKey": kma_auth_key})
            cleaned_data = remove_comment_lines(fetch_data(api_url))
            logging.info(f"Fetched Data:\n{cleaned_data}")
            dataframe = parse_data_to_dataframe(cleaned_data, column_names=get_columns(key="kma_sfctm2"))
            load_to_mysql(mysql_conn_id, dataframe, database=request["database"], table=request["table"])
            return dataframe

        @task(task_id="kma_sfctm2_validation")
        def kma_sfctm2_validation(dataframe: pd.DataFrame, **kwargs) -> None:
            if (dataframe is None) or dataframe.empty:
                logging.warning("kma_sfctm2 DataFrame is empty. Skipping validation.")
                return

        kma_sfctm2_df = kma_sfctm2(_mysql_conn_id, _kma_auth_key)
        kma_sfctm2_df >> kma_sfctm2_validation(kma_sfctm2_df)

    with TaskGroup("nph_aws2_min_group") as nph_aws2_min_group:
        @task(task_id="nph_aws2_min", trigger_rule=TriggerRule.ALL_DONE)
        def nph_aws2_min(mysql_conn_id: str, kma_auth_key: str, **kwargs) -> pd.DataFrame:
            logging.info("지상관측 종관기상관측 AWS 매분자료 조회 AWS 매분자료")
            request = _request_map["nph_aws2_min"]
            tm2 = logical_date_to(kwargs['logical_date'])
            api_url = get_api_url(request, param={"tm2": tm2, "stn": "0", "disp": "0", "help": "0", "authKey": kma_auth_key})
            cleaned_data = remove_comment_lines(fetch_data(api_url))
            dataframe = parse_data_to_dataframe(cleaned_data, column_names=get_columns(key="nph_aws2_min"))
            load_to_mysql(mysql_conn_id, dataframe, database=request["database"], table=request["table"])
            return dataframe

        @task(task_id="nph_aws2_min_validation")
        def nph_aws2_min_validation(dataframe: pd.DataFrame, **kwargs) -> None:
            if (dataframe is None) or dataframe.empty:
                logging.warning("nph_aws2_min DataFrame is empty. Skipping validation.")
                return

        nph_aws2_min_df = nph_aws2_min(_mysql_conn_id, _kma_auth_key)
        nph_aws2_min_df >> nph_aws2_min_validation(nph_aws2_min_df)

    start_task >> branch_hourly() >> [kma_sfctm2_group, skip_kma_sfctm2()] >> nph_aws2_min_group >> end_task


example_crawling_weather()
