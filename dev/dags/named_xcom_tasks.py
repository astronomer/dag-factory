try:
    from airflow.sdk import BaseOperator
except ImportError:
    from airflow.models import BaseOperator


def publish_values(ti):
    result = {"a": [1, 2, 3, 4], "b": [10, 20]}
    for key, value in result.items():
        ti.xcom_push(key=key, value=value)
    return result


def forward_values(value):
    return value


class ValueOperator(BaseOperator):
    template_fields = ("value",)

    def __init__(self, value, **kwargs):
        super().__init__(**kwargs)
        self.value = value

    def execute(self, context):
        self.log.info("Processing value %s", self.value)
        return self.value
