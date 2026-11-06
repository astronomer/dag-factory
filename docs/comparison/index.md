# A workflow definition, not a program

By default, Apache Airflow® users write each workflow, or sequence of tasks, as a Python program. DAG Factory describes
the same workflow as a definition in YAML. Both produce the same Airflow DAG, with the same tasks and the same operators,
so nothing changes when the DAG runs.

What changes is what a reviewer, a tool, or an AI agent has to read. In a Python DAG, the schedule, the tasks, and the
dependencies are spread through code: imports, a `with DAG(...)` block, and the `>>` expressions or function calls at the
end that wire the tasks together. In a DAG Factory file, each of them sits under a fixed key, and each task lists its own
`dependencies`. See [Why DAG Factory?](../why-dag-factory.md) for why that matters and when Python fits better.

The pages below show the same workflow written both ways.

* [Traditional Airflow Operators](traditional_operators.md)
* [TaskFlow API](taskflow_api.md)
