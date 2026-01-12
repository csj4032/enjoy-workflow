import logging
import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite


def build_gx_pandas_validator(dataframe, suite_name: str = "pandas_suite", datasource_name: str = "pandas", asset_name: str = "pandas_asset", batch_def_name: str = "pandas_batch"):
    context = gx.get_context()
    data_source = context.data_sources.add_pandas(name=datasource_name)
    data_asset = data_source.add_dataframe_asset(name=asset_name)
    batch_definition = data_asset.add_batch_definition_whole_dataframe(batch_def_name)
    batch = batch_definition.get_batch(batch_parameters={"dataframe": dataframe})
    context.suites.add(ExpectationSuite(name=suite_name))
    validator = context.get_validator(batch=batch, expectation_suite_name=suite_name)
    return validator


def data_quality_logs(mysql_conn_id: str, dag_id: str, run_id: str, logical_datetime, data) -> None:
    logging.info(f"data: {data}")
    from airflow.providers.mysql.hooks.mysql import MySqlHook
    hook = MySqlHook(mysql_conn_id=mysql_conn_id)
    connection = hook.get_conn()
    logs = get_data_quality_logs(dag_id, run_id, logical_datetime, data)
    logging.info(f"Analysis Logs: {logs}")
    columns_ = ["run_name", "run_id", "entity", "instance", "name", "value", "logical_datetime"]
    insert_query = f"INSERT INTO data_quality_logs ({', '.join(columns_)}) VALUES ({', '.join([f'%({col_})s' for col_ in columns_])}) ON DUPLICATE KEY UPDATE value = VALUES(value)"
    try:
        with connection.cursor() as cursor:
            cursor.executemany(insert_query, logs)
        connection.commit()
    except Exception as e:
        logging.error(f"Error inserting ETL analysis logs: {e}")
        connection.rollback()
        raise
    finally:
        logging.info(f"Finished inserting ETL analysis logs: {connection}")
        connection.close()


def get_data_quality_logs(dag_id, run_id, logical_datetime, data) -> list:
    processed_logs: list[dict] = []
    append_log = processed_logs.append

    def add_log(entity: str, instance: str, name: str, value: float) -> None:
        append_log({"run_name": dag_id, "run_id": run_id, "entity": entity, "instance": instance, "name": name, "value": float(value), "logical_datetime": logical_datetime})

    for expectation in data.get("results", []):
        config = expectation.get("expectation_config") or {}
        result_metrics = expectation.get("result") or {}
        kwargs = config.get("kwargs") or {}

        expectation_type = config.get("type")
        if not expectation_type:
            continue

        if expectation_type == "expect_table_row_count_to_be_between":
            observed = result_metrics.get("observed_value")
            if observed is not None:
                add_log(entity="DataSet", instance="*", name="Size", value=observed)
            continue

        column_name = kwargs.get("column")
        if not column_name:
            continue

        if expectation_type == "expect_column_values_to_not_be_null":
            unexpected_count = result_metrics.get("unexpected_count")
            if unexpected_count is None:
                continue
            completeness = 0.0 if unexpected_count > 0 else 1.0
            add_log(entity="Column", instance=column_name, name="Completeness", value=completeness)
        elif expectation_type == "expect_column_values_to_be_unique":
            unexpected_count = result_metrics.get("unexpected_count")
            missing_count = result_metrics.get("missing_count", 0)
            if unexpected_count is None:
                continue
            uniqueness = 1.0 if (unexpected_count == 0 and (missing_count or 0) == 0) else 0.0
            add_log(entity="Column", instance=column_name, name="Uniqueness", value=uniqueness)
        elif expectation_type == "expect_column_values_to_be_between":
            element_count = result_metrics.get("element_count")
            unexpected_count = result_metrics.get("unexpected_count")
            unexpected_percent = result_metrics.get("unexpected_percent")
            missing_count = result_metrics.get("missing_count")
            missing_percent = result_metrics.get("missing_percent")

            if missing_count is not None:
                add_log(entity="Column", instance=column_name, name="MissingCount", value=missing_count)
            if missing_percent is not None:
                add_log(entity="Column", instance=column_name, name="MissingRate", value=missing_percent / 100.0)

            if unexpected_count is not None:
                add_log(entity="Column", instance=column_name, name="OutOfRangeCount", value=unexpected_count)
            if unexpected_percent is not None:
                add_log(entity="Column", instance=column_name, name="OutOfRangeRate", value=unexpected_percent / 100.0)

            out_rate = None
            if result_metrics.get("unexpected_percent_total") is not None:
                out_rate = result_metrics["unexpected_percent_total"] / 100.0
            elif unexpected_percent is not None:
                out_rate = unexpected_percent / 100.0

            if out_rate is not None:
                add_log(entity="Column", instance=column_name, name="RangeCompliance", value=max(0.0, 1.0 - out_rate))
            if element_count is not None:
                add_log(entity="Column", instance=column_name, name="ElementCount", value=element_count)
        else:
            continue

    return processed_logs
