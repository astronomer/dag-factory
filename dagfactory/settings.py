from airflow.configuration import conf


enable_telemetry = conf.getboolean("dag_factory", "enable_telemetry", fallback=True)
