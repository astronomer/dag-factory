from datetime import datetime, timedelta
from random import randint

try:
    from airflow.providers.standard.operators.python import get_current_context
except ImportError:
    from airflow.operators.python import get_current_context


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


def read_params(params):
    print("params: ", params)
