from airflow.plugins_manager import AirflowPlugin

from dagfactory.listeners import runtime_event


class DagFactoryPlugin(AirflowPlugin):
    name = "Dag Factory Plugin"
    listeners = [runtime_event]


dagfactory_plugin = DagFactoryPlugin()
