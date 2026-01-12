import random
from datetime import datetime, timedelta

import great_expectations as gx
import pandas as pd
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import task, dag, Variable
from faker import Faker
from great_expectations.core.expectation_suite import ExpectationSuite

from common import mmix_slack_operator as slack_operator
from common import mmix_validator as mmix_validator


@dag(dag_id="example_great_expectations",
     default_args={
         "depends_on_past": False,
         "retries": None,
         "retry_delay": timedelta(seconds=10),
     },
     schedule="*/5 * * * *",
     catchup=False,
     start_date=datetime(2026, 1, 1),
     on_success_callback=slack_operator.build_dag_success_callback(Variable.get("mmix-slack-conn-id"), Variable.get("mmix-slack-channel-id")),
     on_failure_callback=slack_operator.build_dag_failure_callback(Variable.get("mmix-slack-conn-id"), Variable.get("mmix-slack-channel-id")),
     tags=["Great Expectations", "Example"])
def example_great_expectations():
    @task
    def data_generation(fake=Faker("ko_KR"), n: int = 1000) -> pd.DataFrame:
        rows = []
        for i in range(1, n + 1):
            rows.append({
                "id": i + 1,
                "name": fake.name(),
                "age": fake.random_int(min=20, max=65),
                "weight": fake.random_int(min=10, max=250),
                "height": fake.random_int(min=10, max=250),
                "gender": random.choice(['남성', '여성']),
                "address": fake.address(),
                "job": fake.job(),
                "email": fake.email(),
                "signup": (datetime.now() - timedelta(days=random.randint(0, 365))).date().isoformat()
            })
        return pd.DataFrame(rows)

    @task(multiple_outputs=True)
    def gx_validation(dataframe: pd.DataFrame, **kwargs) -> None:
        context = gx.get_context()
        data_source = context.data_sources.add_pandas(name="datasource_name")
        data_asset = data_source.add_dataframe_asset(name="asset_name")
        batch_definition = data_asset.add_batch_definition_whole_dataframe("batch_def_name")
        batch = batch_definition.get_batch(batch_parameters={"dataframe": dataframe})
        context.suites.add(ExpectationSuite(name="suite_name"))
        validator = context.get_validator(batch=batch, expectation_suite_name="suite_name")
        validator.expect_table_row_count_to_be_between(0, 0)
        validator.expect_column_values_to_be_unique("id")
        validator.expect_column_values_to_not_be_null("id")
        validator.expect_column_values_to_not_be_null("name")
        validator.expect_column_values_to_not_be_null("email")
        validator.expect_column_values_to_be_between("age", min_value=1, max_value=100)
        mmix_validator.data_quality_logs(Variable.get("mmix-mysql-primary-observability-conn-id"), kwargs["dag"].dag_id, kwargs["run_id"], kwargs["logical_date"], validator.validate())

    start_task = EmptyOperator(task_id="start_empty")
    end_task = EmptyOperator(task_id="end_empty")
    data_generation_task = data_generation(n=random.randint(10, 10000))
    gx_validation_task = gx_validation(data_generation_task)
    start_task >> data_generation_task >> gx_validation_task >> end_task


example_great_expectations()
