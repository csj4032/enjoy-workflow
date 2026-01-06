import io
import logging
import random
from datetime import datetime
from datetime import timedelta

import pandas as pd
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.google.suite.hooks.sheets import GSheetsHook
from airflow.sdk import dag, task
from faker import Faker

_slack_conn_id = Variable.get("mmix-slack-conn-id")
_slack_channel_id = Variable.get("mmix-slack-channel-id")
_aws_conn_id = Variable.get("mmix-aws-conn-id")
_gcp_conn_id = Variable.get("mmix-gcp-conn-id")
_s3_datalakehouse_bucket_name = Variable.get("mmix-aws-s3-datalakehouse-bucket-name")
_customer_google_sheet_id = Variable.get("mmix-customer-google-sheet-id")


def _col_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


@dag(dag_id="example_s3_to_google",
     default_args={
         "start_date": datetime(2026, 1, 1),
         "retries": 0,
         "retry_delay": timedelta(seconds=10),
     },
     schedule=None,
     catchup=False,
     tags=["Stock", "AWS", "GCP", "S3", "Google Drive"])
def example_s3_to_google():
    @task
    def data_generation(fake=Faker("ko_KR"), n: int = 10000) -> pd.DataFrame:
        logging.info(f"Generating {n} rows of fake data ...")
        rows = []
        for i in range(1, n + 1):
            name = fake.name()
            email = fake.email() if random.random() > 0.10 else None
            age = random.randint(18, 65) if random.random() > 0.05 else None
            signup = (datetime.now() - timedelta(days=random.randint(0, 365))).date().isoformat()
            rows.append({
                "id": i,
                "name": name if random.random() > 0.02 else None,
                "email": email,
                "age": age,
                "signup_date": signup
            })
        return pd.DataFrame(rows)

    @task
    def load_to_s3(dataframe: pd.DataFrame, bucket_name: str, object_key: str) -> bytes:
        logging.info(f"Uploading DataFrame to s3://{bucket_name}/{object_key} ...")
        buffer = io.BytesIO()
        dataframe.to_parquet(buffer, engine="pyarrow", index=False)
        buffer.seek(0)
        S3Hook(aws_conn_id=_aws_conn_id).load_bytes(bytes_data=buffer.getvalue(), bucket_name=bucket_name, key=object_key, replace=True)

    @task
    def download_from_s3(bucket_name: str, object_key: str) -> pd.DataFrame:
        s3_object = S3Hook(aws_conn_id=_aws_conn_id).get_key(key=object_key, bucket_name=bucket_name)
        dataframe = pd.read_parquet(io.BytesIO(s3_object.get()["Body"].read()))
        logging.info(f"Downloaded DataFrame from s3://{bucket_name}/{object_key}, shape: {dataframe.shape}")
        return dataframe

    @task
    def load_to_google_drive(dataframe: pd.DataFrame) -> dict:
        hook = GSheetsHook(gcp_conn_id=_gcp_conn_id, api_version="v4")
        values = [dataframe.columns.tolist()] + dataframe.astype(str).values.tolist()
        rows = len(values)
        cols = len(values[0]) if rows else 1
        end_col = _col_letter(cols)
        end_row = rows
        sheet_name = "시트1"
        range_ = f"{sheet_name}!A1:{end_col}{end_row}"
        return hook.update_values(spreadsheet_id=_customer_google_sheet_id, range_=range_, values=values, value_input_option="RAW")

    data_generation_task = data_generation()
    load_to_s3_task = load_to_s3(data_generation_task, _s3_datalakehouse_bucket_name, "data/customer.parquet")
    download_from_s3_task = download_from_s3(_s3_datalakehouse_bucket_name, "data/customer.parquet")
    load_to_google_drive_task = load_to_google_drive(download_from_s3_task)
    (data_generation_task >> load_to_s3_task >> download_from_s3_task >> load_to_google_drive_task)


example_s3_to_google()
