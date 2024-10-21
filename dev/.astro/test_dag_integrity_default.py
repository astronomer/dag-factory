"""Test the validity of all DAGs. **USED BY DEV PARSE COMMAND DO NOT EDIT**"""

import logging
import os
from contextlib import contextmanager

import pytest
from airflow.hooks.base import BaseHook
from airflow.models import Connection, DagBag, Variable
from airflow.utils.db import initdb

# init airflow database
initdb()

# The following code patches errors caused by missing OS Variables, Airflow Connections, and Airflow Variables


# =========== MONKEYPATCH BaseHook.get_connection() ===========
def basehook_get_connection_monkeypatch(key: str, *args, **kwargs):
    print(f"Attempted to fetch connection during parse returning an empty Connection object for {key}")
    return Connection(key)


BaseHook.get_connection = basehook_get_connection_monkeypatch
# # =========== /MONKEYPATCH BASEHOOK.GET_CONNECTION() ===========


# =========== MONKEYPATCH OS.GETENV() ===========
def os_getenv_monkeypatch(key: str, *args, **kwargs):
    default = None
    if args:
        default = args[0]  # os.getenv should get at most 1 arg after the key
    if kwargs:
        default = kwargs.get("default", None)  # and sometimes kwarg if people are using the sig

    env_value = os.environ.get(key, None)

    if env_value:
        return env_value  # if the env_value is set, return it
    if key == "JENKINS_HOME" and default is None:  # fix https://github.com/astronomer/astro-cli/issues/601
        return None
    if default:
        return default  # otherwise return whatever default has been passed
    return f"MOCKED_{key.upper()}_VALUE"  # if absolutely nothing has been passed - return the mocked value


os.getenv = os_getenv_monkeypatch
# # =========== /MONKEYPATCH OS.GETENV() ===========

# =========== MONKEYPATCH VARIABLE.GET() ===========


class magic_dict(dict):
    def __init__(self, *args, **kwargs):
        self.update(*args, **kwargs)

    def __getitem__(self, key):
        return {}.get(key, "MOCKED_KEY_VALUE")


_no_default = object()  # allow falsey defaults


def variable_get_monkeypatch(key: str, default_var=_no_default, deserialize_json=False):
    print(f"Attempted to get Variable value during parse, returning a mocked value for {key}")

    if default_var is not _no_default:
        return default_var
    if deserialize_json:
        return magic_dict()
    return "NON_DEFAULT_MOCKED_VARIABLE_VALUE"


Variable.get = variable_get_monkeypatch
# # =========== /MONKEYPATCH VARIABLE.GET() ===========


@contextmanager
def suppress_logging(namespace):
    """
    Suppress logging within a specific namespace to keep tests "clean" during build
    """
    logger = logging.getLogger(namespace)
    old_value = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = old_value


def get_import_errors():
    """
    Generate a tuple for import errors in the dag bag, and include DAGs without errors.
    """
    with suppress_logging("airflow"):
        dag_bag = DagBag(include_examples=False)

        def strip_path_prefix(path):
            return os.path.relpath(path, os.environ.get("AIRFLOW_HOME"))

        # Initialize an empty list to store the tuples
        result = []

        # Iterate over the items in import_errors
        for k, v in dag_bag.import_errors.items():
            result.append((strip_path_prefix(k), v.strip()))

        # Check if there are DAGs without errors
        for file_path in dag_bag.dags:
            # Check if the file_path is not in import_errors, meaning no errors
            if file_path not in dag_bag.import_errors:
                result.append((strip_path_prefix(file_path), "No import errors"))

        return result


@pytest.mark.parametrize("rel_path, rv", get_import_errors(), ids=[x[0] for x in get_import_errors()])
def test_file_imports(rel_path, rv):
    """Test for import errors on a file"""
    if os.path.exists(".astro/dag_integrity_exceptions.txt"):
        with open(".astro/dag_integrity_exceptions.txt", "r") as f:
            exceptions = f.readlines()
    print(f"Exceptions: {exceptions}")
    if (rv != "No import errors") and rel_path not in exceptions:
        # If rv is not "No import errors," consider it a failed test
        raise Exception(f"{rel_path} failed to import with message \n {rv}")
    else:
        # If rv is "No import errors," consider it a passed test
        print(f"{rel_path} passed the import test")
