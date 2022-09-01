import os


def get_namespace():
    return os.environ.get("AIRFLOW__KUBERNETES__NAMESPACE")


def get_environment():
    return os.environ.get("ENV", "dev")