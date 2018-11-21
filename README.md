# dag-factory

[![Travis CI](https://img.shields.io/travis/ajbosco/dag-factory.svg?style=flat-square)](https://travis-ci.org/ajbosco/dag-factory)
[![Coverage](https://img.shields.io/coveralls/github/ajbosco/dag-factory.svg?style=flat-square)](https://coveralls.io/github/ajbosco/dag-factory)
[![PyPi](https://img.shields.io/pypi/v/dag-factory.svg?style=flat-square)](https://pypi.org/project/dag-factory/)
[![Code Style](https://img.shields.io/badge/code%20style-black-000000.svg?style=flat-square)](https://github.com/ambv/black)

*dag-factory* is a library for dynamically generating [Apache Airflow](https://github.com/apache/incubator-airflow) DAGs from YAML configuration files.
- [Installation](#installation)
- [Usage](#usage)
- [Benefits](#benefits)
- [Contributing](#contributing)
  
## Installation

To install *dag-factory* run `pip install dag-factory`. It requires Python 3.6.0+ and Apache Airflow 1.9+.

## Usage

After installing *dag-factory* in your Airflow environment, there are two steps to creating DAGs. First, we need to create a YAML configuration file. For example:

```
example_dag1:
  default_args:
    owner: 'example_owner'
    start_date: 2018-01-01
  schedule_interval: '0 3 * * *'
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

Then in the DAGs folder in your Airflow environment you need to create a python file like this:

```
from airflow import DAG
import dagfactory

dag_factory = dagfactory.DagFactory("/path/to/dags/config_file.yml")

dag_factory.generate_dags(globals())
```

And this DAG will be generated and ready to run in Airflow!

![screenshot](/img/example_dag.png)

## Benefits

* Construct DAGs without knowing Python
* Construct DAGs without learning Airflow primitives
* Avoid duplicative code
* Everyone loves YAML! ;)

## Contributing

Contributions are welcome! Just submit a Pull Request or Github Issue.