# Python and YAML side by side

By default, Apache Airflow® users write their workflows, or sequences of tasks, in Python.

DAG Factory describes the same workflows in declarative YAML files. Both produce the same Airflow DAG, with the same
tasks and the same operators. What changes is the format of the workflow definition: a Python program, or a fixed YAML
structure that can be checked and updated by tools. See [Why DAG Factory?](../why-dag-factory.md) for when each one fits
better.

This section shows the same workflow written as a plain Airflow Python DAG and as a DAG Factory YAML file.

* [Traditional Airflow Operators](traditional_operators.md)
* [TaskFlow API](taskflow_api.md)
