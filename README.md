# dag-factory

[![Github Actions](https://github.com/ajbosco/dag-factory/workflows/build/badge.svg?branch=master&event=push)](https://github.com/ajbosco/dag-factory/actions?workflow=build)
[![Coverage](https://codecov.io/github/ajbosco/dag-factory/coverage.svg?branch=master)](https://codecov.io/github/ajbosco/dag-factory?branch=master)
[![PyPi](https://img.shields.io/pypi/v/dag-factory.svg)](https://pypi.org/project/dag-factory/)
[![Code Style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![Downloads](https://pepy.tech/badge/dag-factory)](https://pepy.tech/project/dag-factory)

*dag-factory* is a library for dynamically generating [Apache Airflow](https://github.com/apache/incubator-airflow) DAGs from YAML configuration files.
- [Installation](#installation)
- [Usage](#usage)
- [Benefits](#benefits)
- [Contributing](#contributing)
  
## Installation

To install *dag-factory* run `pip install dag-factory`. It requires Python 3.6.0+ and Apache Airflow 2.0+.

## Usage

After installing *dag-factory* in your Airflow environment, there are two steps to creating DAGs. First, we need to create a YAML configuration file. For example:

```yaml
example_dag1:
  default_args:
    owner: 'example_owner'
    start_date: 2018-01-01  # or '2 days'
    end_date: 2018-01-05
    retries: 1
    retry_delay_sec: 300
  schedule_interval: '0 3 * * *'
  concurrency: 1
  max_active_runs: 1
  dagrun_timeout_sec: 60
  default_view: 'tree'  # or 'graph', 'duration', 'gantt', 'landing_times'
  orientation: 'LR'  # or 'TB', 'RL', 'BT'
  description: 'this is an example dag!'
  on_success_callback_name: print_hello
  on_success_callback_file: /usr/local/airflow/dags/print_hello.py
  on_failure_callback_name: print_hello
  on_failure_callback_file: /usr/local/airflow/dags/print_hello.py
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

Then in the DAGs folder in your Airflow environment you need to create a python file like this:

```python
from airflow import DAG
import dagfactory

dag_factory = dagfactory.DagFactory("/path/to/dags/config_file.yml")

dag_factory.clean_dags(globals())
dag_factory.generate_dags(globals())
```

And this DAG will be generated and ready to run in Airflow!

If you have several configuration files you can import them like this:

```python
# 'airflow' word is required for the dagbag to parse this file
from dagfactory import load_yaml_dags

load_yaml_dags(globals_dict=globals(), suffix=['dag.yaml'])
```

![screenshot](/img/example_dag.png)

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
