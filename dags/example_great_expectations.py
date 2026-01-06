import logging
import random
from datetime import datetime, timedelta

import great_expectations as gx
import pandas as pd
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import task, dag, Variable
from faker import Faker
from great_expectations.core.expectation_suite import ExpectationSuite

from common import mmix_slack_operator as slack_operator


@dag(dag_id="example_great_expectations",
     default_args={
         "depends_on_past": False,
         "retries": 1,
         "retry_delay": timedelta(seconds=10),
     },
     schedule=None,
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
    def gx_validation(dataframe: pd.DataFrame, **kwargs) -> str:
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
        logging.info(f"Analysis Logs: {validator.validate()}")

    start_task = EmptyOperator(task_id="start_empty")
    end_task = EmptyOperator(task_id="end_empty")
    data_generation_task = data_generation()
    gx_validation_task = gx_validation(data_generation_task)
    (start_task >> data_generation_task >> gx_validation_task >> end_task)


example_great_expectations()
