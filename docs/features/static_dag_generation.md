# Static DAG File Generation

The `generate_dag_code.py` script is a command-line tool that allows you to convert your `dag-factory` YAML configurations directly into standard Python Airflow DAG files. This is useful for environments where you prefer or require statically defined DAGs, or for inspecting the Python code that `dag-factory` would generate at runtime.

## Purpose

This script facilitates ahead-of-time (AOT) compilation of your YAML-defined workflows into Python DAG files. These generated files can then be processed by Airflow like any other manually created DAG file.

Key benefits include:
-   **Static DAGs:** Satisfy environments that do not allow dynamic DAG generation from YAML at runtime.
-   **Code Inspection:** Allows you to see the Python representation of your YAML configuration, which can be helpful for debugging or understanding `dag-factory`'s translation.
-   **Migration:** Can be used as a step in migrating from YAML-based configurations to pure Python DAGs if needed.
-   **Performance:** In some scenarios, parsing static Python files might be marginally faster for Airflow than processing YAML configurations at runtime, especially for a very large number of DAGs.

## Prerequisites

Before using the script, ensure the following:
-   `dag-factory` is installed in your Python environment.
-   `libcst` is installed. The script uses LibCST to construct the Python code. If you installed `dag-factory` without this specific dependency, you might need to install it separately:
    ```bash
    pip install libcst
    ```

## Usage

The script is executed as a Python module. You need to provide the path to your `dag-factory` YAML configuration file.

**Basic Command Structure:**

```bash
python -m dagfactory.generate_dag_code path/to/your/dag_config.yml
```

**Arguments:**

-   `config_file`: (Required) The path to the YAML configuration file containing your DAG definitions.

**Output:**

The script prints the generated Python DAG code to standard output.

**Saving to a File:**

To save the generated code to a Python file, redirect the standard output:

```bash
python -m dagfactory.generate_dag_code path/to/your/dag_config.yml > path/to/your/generated_dag.py
```
This `generated_dag.py` can then be placed in your Airflow DAGs folder.

## Example

Let's say you have a simple YAML configuration file named `my_simple_dag.yml`:

```yaml
# my_simple_dag.yml
my_dag_from_yaml:
  default_args:
    owner: 'airflow'
    start_date: 2023-10-26
  schedule_interval: '@daily'
  description: 'A simple DAG generated statically.'
  tasks:
    task_one:
      operator: airflow.operators.bash.BashOperator
      bash_command: 'echo "Hello from Task One"'
    task_two:
      operator: airflow.operators.bash.BashOperator
      bash_command: 'echo "Hello from Task Two"'
      dependencies: [task_one]
```

**Command to Generate Python Code:**

```bash
python -m dagfactory.generate_dag_code my_simple_dag.yml > my_generated_dag_from_yaml.py
```

**Resulting `my_generated_dag_from_yaml.py` (snippet):**

The script will produce Python code similar to this (formatting and exact import structure might vary slightly):

```python
import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# Default DAG arguments
default_args = {"owner": "airflow", "start_date": datetime.datetime(2023, 10, 26)}

# DAG Definition
with DAG(
    "my_dag_from_yaml",
    default_args=default_args,
    schedule_interval="@daily",
    description="A simple DAG generated statically.",
    catchup=True, # Default unless specified otherwise in YAML
) as dag:
    task_one = BashOperator(
        task_id="task_one", bash_command='echo "Hello from Task One"'
    )
    task_two = BashOperator(
        task_id="task_two", bash_command='echo "Hello from Task Two"'
    )

    task_one >> task_two
```

You can then place `my_generated_dag_from_yaml.py` into your Airflow DAGs folder.
