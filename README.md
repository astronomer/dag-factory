# dag-factory

[![Github Actions](https://github.com/ajbosco/dag-factory/workflows/build/badge.svg?branch=master&event=push)](https://github.com/ajbosco/dag-factory/actions?workflow=build)
[![Coverage](https://codecov.io/github/ajbosco/dag-factory/coverage.svg?branch=master)](https://codecov.io/github/ajbosco/dag-factory?branch=master)
[![PyPi](https://img.shields.io/pypi/v/dag-factory.svg)](https://pypi.org/project/dag-factory/)
[![Code Style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![Downloads](https://pepy.tech/badge/dag-factory)](https://pepy.tech/project/dag-factory)

Welcome to *dag-factory*! *dag-factory* is a library for [Apache Airflow](https://github.com/apache/incubator-airflow) to construct DAGs declaratively via configuration files. 

The minimum requirements for **dag-factory** are:
- Python 3.6.0+
- Apache Airflow 2.0+

For a gentle introduction, please take a look at our [Quickstart Guide](#quickstart). For more examples, please see the [examples](/examples) folder.

- [Quickstart](#quickstart)
- [Features](#features)
  - [Multiple Configuration Files](#multiple-configuration-files)
  - [Dynamically Mapped Tasks](#dynamically-mapped-tasks)
  - [Datasets](#datasets)
  - [Custom Operators](#custom-operators)
- [Benefits](#benefits)
- [Notes](#notes)
  - [HttpSensor (since 0.10.0)](#httpsensor-since-0100)
- [Contributing](#contributing)
  
## Quickstart

The following example demonstrates how to create a simple DAG using *dag-factory*. We will be generating a DAG with three tasks, where `task_2` and `task_3` depend on `task_1`. 
These tasks will be leveraging the `BashOperator` to execute simple bash commands.

![screenshot](/img/quickstart_dag.png)

1. To install *dag-factory*, run the following pip command in your Airflow environment:
```bash
pip install dag-factory
```

2. Create a YAML configuration file called `config_file.yml` and save it within your airflow dags folder:
```yaml
example_dag1:
  default_args:
    owner: 'example_owner'
    retries: 1
    start_date: '2024-01-01'
  schedule_interval: '0 3 * * *'
  catchup: False
  description: 'this is an example dag!'
  tasks:
    task_1:
      operator: airflow.operators.bash_operator.BashOperator
      bash_command: 'echo 1'
    task_2:
      operator: airflow.operators.bash_operator.BashOperator
      bash_command: 'echo 2'
      dependencies: [task_1]
    task_3:
      operator: airflow.operators.bash_operator.BashOperator
      bash_command: 'echo 3'
      dependencies: [task_1]
```
We are setting the execution order of the tasks by specifying the `dependencies` key.

3. In the same folder, create a python file called `generate_dags.py`. This file is responsible for generating the DAGs from the configuration file and is a one-time setup. 
You won't need to modify this file unless you want to add more configuration files or change the configuration file name.

```python
from airflow import DAG  ## by default, this is needed for the dagbag to parse this file
import dagfactory
from pathlib import Path

config_file = Path.cwd() / "dags/config_file.yml"
dag_factory = dagfactory.DagFactory(config_file)

dag_factory.clean_dags(globals())
dag_factory.generate_dags(globals())
```

After a few moments, the DAG will be generated and ready to run in Airflow. Unpause the DAG in the Airflow UI and watch the tasks execute!

![screenshot](/img/quickstart_gantt.png)

Please look at the [examples](/examples) folder for more examples.

## Features

### Multiple Configuration Files
If you want to split your DAG configuration into multiple files, you can do so by leveraging a suffix in the configuration file name.
```python
# 'airflow' word is required for the dagbag to parse this file
from dagfactory import load_yaml_dags

load_yaml_dags(globals_dict=globals(), suffix=['dag.yaml'])
```

### Dynamically Mapped Tasks
If you want to create a dynamic number of tasks, you can use the `mapped_tasks` key in the configuration file. The `mapped_tasks` key is a list of dictionaries, where each dictionary represents a task. 

```yaml
...
  tasks:
    request:
      operator: airflow.operators.python.PythonOperator
      python_callable_name: example_task_mapping
      python_callable_file: /usr/local/airflow/dags/expand_tasks.py # this file should contain the python callable
    process:
      operator: airflow.operators.python_operator.PythonOperator
      python_callable_name: expand_task
      python_callable_file: /usr/local/airflow/dags/expand_tasks.py
      partial:
        op_kwargs:
          test_id: "test"
      expand:
        op_args:
          request.output
      dependencies: [request]
```
![mapped_tasks_example.png](img/mapped_tasks_example.png)

### Datasets
**dag-factory** supports scheduling DAGs via [Apache Airflow Datasets](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/datasets.html).

To leverage, you need to specify the `Dataset` in the `outlets` key in the configuration file. The `outlets` key is a list of strings that represent the dataset locations.
In the `schedule` key of the consumer dag, you can set the `Dataset` you would like to schedule against. The key is a list of strings that represent the dataset locations. 
The consumer dag will run when all the datasets are available.

```yaml
producer_dag:
  default_args:
    owner: "example_owner"
    retries: 1
    start_date: '2024-01-01'
  description: "Example DAG producer simple datasets"
  schedule_interval: "0 5 * * *"
  tasks:
    task_1:
      operator: airflow.operators.bash_operator.BashOperator
      bash_command: "echo 1"
      outlets: [ 's3://bucket_example/raw/dataset1.json' ]
    task_2:![custom_operators.png](..%2F..%2FDesktop%2Fcustom_operators.png)
      operator: airflow.operators.bash_operator.BashOperator
      bash_command: "echo 2"
      dependencies: [ task_1 ]
      outlets: [ 's3://bucket_example/raw/dataset2.json' ]
consumer_dag:
  default_args:
    owner: "example_owner"
    retries: 1
    start_date: '2024-01-01'
  description: "Example DAG consumer simple datasets"
  schedule: [ 's3://bucket_example/raw/dataset1.json', 's3://bucket_example/raw/dataset2.json' ]
  tasks:
    task_1:
      operator: airflow.operators.bash_operator.BashOperator
      bash_command: "echo 'consumer datasets'"
```
![datasets_example.png](img/datasets_example.png)
 
### Custom Operators
**dag-factory** supports using custom operators. To leverage, set the path to the custom operator within the `operator` key in the configuration file. You can add any additional parameters that the custom operator requires.

```yaml
...
  tasks:
    begin:
      operator: airflow.operators.dummy_operator.DummyOperator
    make_bread_1:
      operator: customized.operators.breakfast_operators.MakeBreadOperator
      bread_type: 'Sourdough'
```
![custom_operators.png](img/custom_operators.png)
## Notes

### HttpSensor (since 0.10.0)

The package `airflow.sensors.http_sensor` works with all supported versions of Airflow. In Airflow 2.0+, the new package name can be used in the operator value: `airflow.providers.http.sensors.http`

The following example shows `response_check` logic in a python file:

```yaml
task_2:
  operator: airflow.sensors.http_sensor.HttpSensor
  http_conn_id: 'test-http'
  method: 'GET'
  response_check_name: check_sensor
  response_check_file: /path/to/example1/http_conn.py
  dependencies: [task_1]
```

The `response_check` logic can also be provided as a lambda:

```yaml
task_2:
  operator: airflow.sensors.http_sensor.HttpSensor
  http_conn_id: 'test-http'
  method: 'GET'
  response_check_lambda: 'lambda response: "ok" in reponse.text'
  dependencies: [task_1]
```

## Benefits

* Construct DAGs without knowing Python
* Construct DAGs without learning Airflow primitives
* Avoid duplicative code
* Everyone loves YAML! ;)

## Contributing

Contributions are welcome! Just submit a Pull Request or Github Issue.
