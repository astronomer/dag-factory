from airflow import DAG
import dagfactory

config_file = "/usr/local/airflow/dags/example_dag_factory.yml"
example_dag_factory = dagfactory.DagFactory(config_file)

example_dag_factory.clean_dags(globals())
example_dag_factory.generate_dags(globals())