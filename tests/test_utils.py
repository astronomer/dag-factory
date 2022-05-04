import datetime
import os

import pendulum
import pytest

from dagfactory import utils


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

    python_callable = utils.get_python_callable(
        python_callable_name, python_callable_file
    )

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
