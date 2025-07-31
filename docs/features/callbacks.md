# Callbacks

DAG Factory supports the use of callbacks. These callbacks can be set at the DAG, TaskGroup, or Task level. The way
that callbacks that can be configured for DAGs, TaskGroups, and Tasks differ slightly, and details around this can be
found in the [Apache Airflow documentation](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/callbacks.html#).

Within DAG Factory itself, there are three approaches to defining callbacks. The goal is to make this process
intuitive and provide parity with the traditional DAG authoring experience. These approaches to configure callbacks
are outlined below, each with an example of implementation. While proceeding examples are all defined for individual
Tasks, callbacks can also be defined using `default_args`, or at the DAG and TaskGroup level.

* [Passing a string that points to a callable](#passing-a-string-that-points-to-a-callable)
* [Specifying a user-defined `.py` and the function within that file to be executed](#specifying-a-user-defined-py-file-and-function)
* [Configuring callbacks from providers](#provider-callbacks)

## Passing a string that points to a callable

The most traditional way of configuring callbacks is by defining a custom function within the Airflow project and
assigning that callback to the desired Task. Using the syntax below, this can be implemented using DAG Factory. In this
case, the `output_standard_message` function is a user-defined function stored in the `include/custom_callbacks.py`
file. This function requires no parameters, and the YAML would take the form below.

For this example to be implemented in DAG Factory, the `include/custom_callbacks.py` file must be on the Python
`sys.path`. If this is not the case, the full path to a `.py` function can be specified, as shown below.

```yaml
...

  - task_id: task_1
    operator: airflow.operators.bash_operator.BashOperator
    bash_command: "echo task_1"
    on_failure_callback: include.custom_callbacks.output_standard_message
...
```

Sometimes, a function may have parameters that need to be defined within the Task itself. Here, the
`output_custom_message` callback takes two key-word arguments; `param1`, and `param2`. These values are defined in the
YAML itself, offering DAG Factory authors an additional degree of flexibility and verbosity.

```yaml
...

  - task_id: task_2
    operator: airflow.operators.bash_operator.BashOperator
    bash_command: "echo task_2"
    on_success_callback:
      callback: include.custom_callbacks.output_custom_message
      param1: "Task status"
      param2: "Successful!"
...
```

## Specifying a user-defined `.py` file and function

In addition to passing a string that points to a callback, the full path to the file and name of the callback can be
specified for a DAG, TaskGroup, or Task. This provides a viable option for defining a callback when the director the
`.py` file is stored in is not on the Python path.

```yaml
...

  - task_id: task_3
    operator: airflow.operators.bash_operator.BashOperator
    bash_command: "echo task_3"
    on_retry_callback_name: output_standard_message
    on_retry_callback_file: /usr/local/airflow/include/custom_callbacks.py
...
```

Note that this method for defining callbacks in DAG Factory does not allow for parameters to be passed to the callable
within the YAML itself.

## Provider callbacks

In addition to custom-built callbacks, there are a number of provider-built callbacks that can be used when defining a
DAG. With DAG Factory, these callbacks can be configured similar to how they would be when authoring a traditional DAG.
First, the type of callback is specified (`on_success_callback`, `on_failure_callback`, etc.). The `callback` key-value
pair specifies the provider-built function to be executed. Then, the specific key-word arguments the callback takes can
be specified, as shown below.

Note that the provider package being used must be available on the Python `sys.path` path, meaning it may need to be
`pip installed`.

```yaml
...
  - task_id: task_4
    operator: airflow.operators.bash_operator.BashOperator
    bash_command: "echo task_4"
    on_failure_callback:
      callback: airflow.providers.slack.notifications.slack.send_slack_notification
      slack_conn_id: slack_conn_id
      text: |
        :red_circle: Task Failed.
        This task has failed and needs to be addressed.
        Please remediate this issue ASAP.
      channel: "#channel"
...
```
