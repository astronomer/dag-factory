import csv
import os
from datetime import datetime, timedelta
from random import randint
from typing import Any

try:
    from airflow.providers.standard.operators.python import get_current_context
except ImportError:
    from airflow.operators.python import get_current_context

try:
    from airflow.sdk import ObjectStoragePath
except ImportError:
    from airflow.io.path import ObjectStoragePath


def build_numbers_list():
    return [2, 4, 6]


def some_number():
    return randint(0, 100)


def double(number: int):
    result = 2 * number
    print(result)
    return result


def multiply(a: int, b: int) -> int:
    result = a * b
    print(result)
    return result


#    added_values = add.expand(x=first_list(), y=second_list())


def double_with_label(number: int, label: bool = False):
    result = 2 * number
    if not label:
        print(result)
        return result
    else:
        label_info = "even" if number % 2 else "odd"
        print(f"{result} is {label_info}")
        return result, label_info


def extract_last_name(full_name: str):
    name, last_name = full_name.split(" ")
    print(f"{name} {last_name}")
    context = get_current_context()
    context["custom_mapping_key"] = name
    return last_name


def one_day_ago(execution_date: datetime):
    return execution_date - timedelta(days=1)


def read_params(params: dict[str, Any]) -> None:
    print("params: ", params)
    print("model_version:", params["model_version"])
    print("my_param:", params["my_param"])


def generate_data():
    print("Produced data to file:///$AIRFLOW_HONE/data.csv")
    data_dir = os.environ.get("AIRFLOW_HONE", "/usr/local/airflow")
    file_path = os.path.join(data_dir, "data.csv")

    with open(file_path, "w") as f:
        f.write("id,value\n1,42\n2,43\n")

    print(f"Produced data to file://{file_path}")


def object_storage_ops(my_obj_storage: ObjectStoragePath) -> None:
    assert isinstance(my_obj_storage, ObjectStoragePath)
    with my_obj_storage.open("rb") as f:
        text = f.read().decode("utf-8")
        reader = csv.reader(text.splitlines(), delimiter=",")
        rows = list(reader)
        print(rows)
