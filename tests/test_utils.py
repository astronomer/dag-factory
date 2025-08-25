import datetime
import os
from unittest.mock import patch

import pendulum
import pytest

from dagfactory import utils
from dagfactory.utils import cast_with_type

NOW = datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
CET = pendulum.timezone("Europe/Amsterdam")
UTC = pendulum.timezone("UTC")


def test_get_start_date_date_no_timezone():
    expected = datetime.datetime(2018, 2, 1, 0, 0, tzinfo=UTC)
    actual = utils.get_datetime(datetime.date(2018, 2, 1))
    assert actual == expected


def test_get_start_date_datetime_no_timezone():
    expected = datetime.datetime(2018, 2, 1, 5, 4, tzinfo=UTC)
    actual = utils.get_datetime(datetime.datetime(2018, 2, 1, 5, 4))
    assert actual == expected


def test_get_start_date_relative_time_no_timezone():
    expected = NOW.replace(tzinfo=UTC) - datetime.timedelta(days=1)
    actual = utils.get_datetime("1 day")
    assert actual == expected


def test_get_start_date_date_timezone():
    expected = datetime.datetime(2018, 2, 1, 0, 0, tzinfo=CET)
    actual = utils.get_datetime(datetime.date(2018, 2, 1), "Europe/Amsterdam")
    assert actual == expected


def test_get_start_date_datetime_timezone():
    expected = datetime.datetime(2018, 2, 1, 0, 0, tzinfo=CET)
    actual = utils.get_datetime(datetime.datetime(2018, 2, 1), "Europe/Amsterdam")
    assert actual == expected


def test_get_start_date_relative_time_timezone():
    expected = NOW.replace(tzinfo=CET) - datetime.timedelta(days=1)
    actual = utils.get_datetime("1 day", "Europe/Amsterdam")
    assert actual == expected


def test_get_start_date_bad_timezone():
    with pytest.raises(Exception):
        utils.get_datetime(datetime.datetime(2018, 2, 1), "bad_timezone")


def test_get_start_date_bad_date():
    with pytest.raises(Exception):
        utils.get_datetime("bad_date")


def test_get_time_delta_seconds():
    expected = datetime.timedelta(0, 25)
    actual = utils.get_time_delta("25 seconds")
    assert actual == expected


def test_get_time_delta_minutes():
    expected = datetime.timedelta(0, 60)
    actual = utils.get_time_delta("1 minute")
    assert actual == expected


def test_get_time_delta_hours():
    expected = datetime.timedelta(0, 18000)
    actual = utils.get_time_delta("5 hours")
    assert actual == expected


def test_get_time_delta_days():
    expected = datetime.timedelta(10)
    actual = utils.get_time_delta("10 days")
    assert actual == expected


def test_get_time_delta_combo():
    expected = datetime.timedelta(0, 3600)
    actual = utils.get_time_delta("1 hour 30 minutes")
    assert actual == expected


def test_get_time_delta_bad_date():
    with pytest.raises(Exception):
        utils.get_time_delta("bad_date")


def test_merge_configs_same_configs():
    dag_config = {"thing": "value1"}
    default_config = {"thing": "value2"}

    expected = {"thing": "value1"}
    actual = utils.merge_configs(dag_config, default_config)
    assert actual == expected


def test_merge_configs_different_configs():
    dag_config = {"thing": "value1"}
    default_config = {"thing2": "value2"}

    expected = {"thing": "value1", "thing2": "value2"}
    actual = utils.merge_configs(dag_config, default_config)
    assert actual == expected


def test_merge_configs_nested_configs():
    dag_config = {"thing": {"thing3": "value3"}}
    default_config = {"thing2": "value2"}

    expected = {"thing": {"thing3": "value3"}, "thing2": "value2"}
    actual = utils.merge_configs(dag_config, default_config)
    assert actual == expected


def print_test():
    print("test")


def test_get_python_callable_valid():
    python_callable_file = os.path.realpath(__file__)
    python_callable_name = "print_test"

    python_callable = utils.get_python_callable(python_callable_name, python_callable_file)

    assert callable(python_callable)


def test_get_python_callable_invalid_path():
    python_callable_file = "not/absolute/path"
    python_callable_name = "print_test"

    with pytest.raises(Exception):
        utils.get_python_callable(python_callable_name, python_callable_file)


def test_get_python_callable_missing_param_file():
    python_callable_file = None
    python_callable_name = "print_test"

    with pytest.raises(Exception):
        utils.get_python_callable(python_callable_name, python_callable_file)


def test_get_python_callable_missing_param_name():
    python_callable_file = "/not/absolute/path"
    python_callable_name = None

    with pytest.raises(Exception):
        utils.get_python_callable(python_callable_name, python_callable_file)


def test_get_python_callable_lambda_valid():
    lambda_expr = "lambda a: a"

    python_callable = utils.get_python_callable_lambda(lambda_expr)

    assert callable(python_callable)


def test_get_python_callable_lambda_works():
    lambda_expr = "lambda a: a"

    python_callable = utils.get_python_callable_lambda(lambda_expr)

    assert callable(python_callable)
    assert python_callable("xyz") == "xyz"
    assert python_callable(5) == 5


def test_get_python_callable_lambda_invalid_expr():
    lambda_expr = "invalid lambda expr"

    with pytest.raises(Exception):
        utils.get_python_callable_lambda(lambda_expr)


def test_get_python_callable_non_lambda_valid_expr():
    lambda_expr = """
    def fun():
        print('hello')
    """

    with pytest.raises(Exception):
        utils.get_python_callable_lambda(lambda_expr)


def test_get_python_callable_lambda_missing_param():
    lambda_expr = None

    with pytest.raises(Exception):
        utils.get_python_callable_lambda(lambda_expr)


def test_get_start_date_date_string():
    expected = datetime.datetime(2018, 2, 1, 0, 0, tzinfo=UTC)
    actual = utils.get_datetime("2018-02-01")
    assert actual == expected


def test_get_expand_partial_kwargs_with_expand_and_partial():
    task_params = {
        "task_id": "my_task",
        "expand": {"key_1": "value_1"},
        "partial": {"key_2": {"nested_key_1": "nested_value_1"}},
    }
    expected_expand_kwargs = {"key_1": "value_1"}
    expected_partial_kwargs = {"key_2": {"nested_key_1": "nested_value_1"}}
    expected_task_params = {"task_id": "my_task"}

    result_task_params, result_expand_kwargs, result_partial_kwargs = utils.get_expand_partial_kwargs(task_params)
    assert result_expand_kwargs == expected_expand_kwargs
    assert result_partial_kwargs == expected_partial_kwargs
    assert result_task_params == expected_task_params


def test_get_expand_partial_kwargs_without_partial():
    task_params = {
        "task_id": "task2",
        "expand": {"param1": "value1", "param2": "value2"},
    }
    expected_result = (
        {"task_id": "task2"},
        {"param1": "value1", "param2": "value2"},
        {},
    )
    assert utils.get_expand_partial_kwargs(task_params) == expected_result


def test_is_partial_duplicated():
    partial_kwargs = {"key_1": "value_1", "key_2": "value_2"}
    task_params = {"key_3": "value_3", "key_4": "value_4"}

    assert utils.is_partial_duplicated(partial_kwargs, task_params) == False

    partial_kwargs = {"key_1": "value1", "key_3": "value3"}
    task_params = {"key_3": "value3", "key_4": "value4"}
    try:
        utils.is_partial_duplicated(partial_kwargs, task_params)
    except Exception as e:
        assert str(e) == "Duplicated partial kwarg! It's already in task_params."


def test_open_and_filter_yaml_config_datasets():
    datasets_names = ["dataset_custom_1", "dataset_custom_2"]
    file_path = "dev/dags/datasets/example_config_datasets.yml"

    actual = utils.get_datasets_uri_yaml_file(file_path, datasets_names)
    expected = [
        "s3://bucket-cjmm/raw/dataset_custom_1",
        "s3://bucket-cjmm/raw/dataset_custom_2",
    ]

    assert actual == expected


def get_datasets_map_uri_yaml_file():
    datasets_names = ["dataset_custom_1", "dataset_custom_2"]
    file_path = "dev/dags/datasets/example_config_datasets.yml"

    actual = utils.get_datasets_uri_yaml_file(file_path, datasets_names)
    expected = {
        "dataset_custom_1": "s3://bucket-cjmm/raw/dataset_custom_1",
        "dataset_custom_2": "s3://bucket-cjmm/raw/dataset_custom_2",
    }

    assert actual == expected


def test_valid_uri():
    actual = utils.make_valid_variable_name("s3://bucket/dataset")
    expected = "s3___bucket_dataset"
    assert actual == expected


def test_uri_with_special_characters():
    actual = utils.make_valid_variable_name("s3://bucket/dataset-1!@#$%^&*()")
    expected = "s3___bucket_dataset_1__________"
    assert actual == expected


def test_uri_starting_with_number():
    actual = utils.make_valid_variable_name("123/bucket/dataset")
    expected = "_123_bucket_dataset"
    assert actual == expected


def test_open_and_filter_yaml_config_datasets_file_notfound():
    datasets_names = ["dataset_custom_1", "dataset_custom_2"]
    file_path = "examples/datasets/not_found_example_config_datasets.yml"

    with pytest.raises(Exception):
        utils.get_datasets_uri_yaml_file(file_path, datasets_names)

def test_get_json_serialized_callable_dict():
    # Test with dictionary input
    data = {"key1": "value1", "key2": 123}
    serializer = utils.get_json_serialized_callable(data)

    # Verify it's callable
    assert callable(serializer)

    # Verify it returns correctly serialized JSON
    result = serializer()
    assert isinstance(result, str)
    assert '"key1": "value1"' in result
    assert '"key2": 123' in result


def test_get_json_serialized_callable_valid_json_string():
    # Test with a valid JSON string
    json_str = '{"key1": "value1", "key2": 123}'
    serializer = utils.get_json_serialized_callable(json_str)

    # Verify it returns the same string
    result = serializer()
    assert result == json_str


def test_get_json_serialized_callable_invalid_json_string():
    # Test with an invalid JSON string
    invalid_json = "{key1: value1, key2: 123}"  # Missing quotes

    # Should raise ValueError
    with pytest.raises(ValueError):
        utils.get_json_serialized_callable(invalid_json)


def test_get_json_serialized_callable_non_dict_non_str():
    # Test with integer - should raise TypeError
    with pytest.raises(TypeError):
        utils.get_json_serialized_callable(123)

    # Test with list - should raise TypeError
    with pytest.raises(TypeError):
        utils.get_json_serialized_callable([1, 2, 3])


def test_get_json_serialized_callable_accepts_kwargs():
    # The callable should accept arbitrary kwargs without affecting the result
    data = {"key1": "value1"}
    serializer = utils.get_json_serialized_callable(data)

    # Call with unused kwargs
    result = serializer(unused_param="test", another_param=123)
    assert '"key1": "value1"' in result


class DummyClass:
    def __init__(self, a=None, b=None):
        self.a = a
        self.b = b

    def __eq__(self, other):
        return isinstance(other, DummyClass) and self.a == other.a and self.b == other.b


class DummyClassWithArgs:
    def __init__(self, a1, a2, a=None, b=None):
        self.a1 = a1
        self.a2 = a2
        self.a = a
        self.b = b

    def __eq__(self, other):
        return (
            isinstance(other, DummyClassWithArgs)
            and self.a1 == other.a1
            and self.a2 == other.a2
            and self.a == other.a
            and self.b == other.b
        )


class TestCustomType:

    @patch("dagfactory.utils._import_from_string")
    def test_cast_simple_dict_with_type(self, mock_import):
        mock_import.return_value = DummyClass
        data = {"__type__": "path.to.DummyClass", "a": 1, "b": 2}
        result = cast_with_type(data)
        assert result == DummyClass(a=1, b=2)

    @patch("dagfactory.utils._import_from_string")
    def test_nested_dict(self, mock_import):
        mock_import.return_value = DummyClass
        data = {"__type__": "path.to.DummyClass", "a": {"__type__": "path.to.DummyClass", "a": 10, "b": 20}, "b": 5}
        result = cast_with_type(data)
        assert result == DummyClass(a=DummyClass(a=10, b=20), b=5)

    def test_list_without_type(self):
        data = [1, 2, {"x": 3}]
        result = cast_with_type(data)
        assert result == [1, 2, {"x": 3}]

    @patch("dagfactory.utils._import_from_string")
    def test_typed_list(self, mock_import):
        mock_import.return_value = DummyClass
        data = {
            "__type__": "builtins.list",
            "items": [
                {"__type__": "path.to.DummyClass", "a": 1, "b": 2},
                {"__type__": "path.to.DummyClass", "a": 3, "b": 4},
            ],
        }
        result = cast_with_type(data)
        expected = [DummyClass(a=1, b=2), DummyClass(a=3, b=4)]
        assert result == expected

    def test_non_dict_non_list(self):
        assert cast_with_type("hello") == "hello"
        assert cast_with_type(123) == 123

    def test_type_args(self):
        data = {
            "__type__": "tests.test_utils.DummyClassWithArgs",
            "__args__": {
                "__type__": "builtins.list",
                "items": [1, {"__type__": "tests.test_utils.DummyClass", "a": 1, "b": 2}],
            },
            "a": 1,
            "b": {"__type__": "tests.test_utils.DummyClass", "a": 3, "b": 4},
        }

        result = cast_with_type(data)
        expected = DummyClassWithArgs(
            1,
            DummyClass(a=1, b=2),
            a=1,
            b=DummyClass(a=3, b=4),
        )
        assert result == expected
