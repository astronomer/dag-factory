# The following import is here so Airflow parses this file
# from airflow import DAG
import dagfactory

example_dag_factory = dagfactory.DagFactory(
    config_filepath="/Users/pankaj/Documents/astro_code/dag-factory/scripts/airflow3/dags/kpo.yml"
)


# Creating task dependencies
example_dag_factory.generate_dags(globals())
