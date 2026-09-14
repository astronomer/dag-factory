import datetime
import os
from unittest.mock import patch

import pytest

from dagfactory._yaml import load_yaml_file


def write_config(tmp_path, content):
    path = tmp_path / "config.yml"
    path.write_text(content)
    return load_yaml_file(str(path))


def test_load_yaml_file_plain_mapping(tmp_path):
    assert write_config(tmp_path, "a: 1\nb: two\n") == {"a": 1, "b": "two"}


@patch.dict(os.environ, {"DAGFACTORY_TEST_OWNER": "team-a"})
def test_load_yaml_file_expands_env_vars(tmp_path):
    assert write_config(tmp_path, "owner: $DAGFACTORY_TEST_OWNER\n") == {"owner": "team-a"}


def test_load_yaml_file_leaves_unset_env_var_untouched(tmp_path):
    assert write_config(tmp_path, "owner: $DAGFACTORY_DEFINITELY_UNSET\n") == {"owner": "$DAGFACTORY_DEFINITELY_UNSET"}


def test_load_yaml_file_casts_type(tmp_path):
    config = write_config(tmp_path, "retry_delay:\n  __type__: datetime.timedelta\n  seconds: 300\n")
    assert config["retry_delay"] == datetime.timedelta(seconds=300)


def test_load_yaml_file_casts_nested_type(tmp_path):
    config = write_config(
        tmp_path,
        "default_args:\n  retry_delay:\n    __type__: datetime.timedelta\n    seconds: 60\n",
    )
    assert config["default_args"]["retry_delay"] == datetime.timedelta(seconds=60)


def test_load_yaml_file_join(tmp_path):
    config = write_config(tmp_path, "uri:\n  __join__:\n    - s3://bucket/\n    - prefix\n")
    assert config["uri"] == "s3://bucket/prefix"


def test_load_yaml_file_join_stringifies_non_strings(tmp_path):
    config = write_config(tmp_path, "label:\n  __join__:\n    - run-\n    - 42\n")
    assert config["label"] == "run-42"


@pytest.mark.parametrize("operator,separator", [("__and__", "&"), ("__or__", "|")])
def test_load_yaml_file_logical_operator_falls_back_to_string(tmp_path, operator, separator):
    # Plain strings support neither & nor |, so the helper formats them instead.
    config = write_config(tmp_path, f"schedule:\n  {operator}:\n    - dataset_a\n    - dataset_b\n")
    assert config["schedule"] == f"(dataset_a {separator} dataset_b)"


def test_load_yaml_file_nested_logical_operators(tmp_path):
    config = write_config(
        tmp_path,
        "schedule:\n  __and__:\n    - dataset_a\n    - __or__:\n        - dataset_b\n        - dataset_c\n",
    )
    assert config["schedule"] == "(dataset_a & (dataset_b | dataset_c))"


def test_load_yaml_file_logical_operator_inside_list(tmp_path):
    config = write_config(tmp_path, "items:\n  - __join__:\n      - a\n      - b\n  - plain\n")
    assert config["items"] == ["ab", "plain"]


def test_load_yaml_file_leaves_other_keys_alone(tmp_path):
    config = write_config(tmp_path, "tasks:\n  t1:\n    operator: SomeOperator\n    bash_command: echo hi\n")
    assert config["tasks"]["t1"] == {"operator": "SomeOperator", "bash_command": "echo hi"}


def test_load_yaml_file_missing_file(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_yaml_file(str(tmp_path / "nope.yml"))
