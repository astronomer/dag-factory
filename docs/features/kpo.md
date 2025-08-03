# KubernetesPodOperator

In DAG Factory, you can use [Airflow Kubernetes Provider](https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/operators.html) to create and run Pods on a Kubernetes cluster.

## Example DAG

First, define your DAG configuration in a YAML file

```yaml
--8<-- "dev/dags/kpo.yml"
```

Then, you can load this YAML configuration dynamically.

```python
--8<-- "dev/dags/kpo.py"
```
