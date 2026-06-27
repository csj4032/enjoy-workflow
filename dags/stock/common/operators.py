from airflow.models import BaseOperator


class KoreaStockOperator(BaseOperator):
    """Base operator for Korea stock market tasks."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def execute(self, context):
        raise NotImplementedError
