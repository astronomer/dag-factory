from __future__ import annotations

import copy
import os
from functools import reduce

import yaml

from .utils import cast_with_type


def load_yaml_file(file_path: str) -> dict[str, any]:
    """
    Load a YAML file into a dictionary.
    """

    def _flatten_logical_expressions_helper(data):
        if isinstance(data, str):
            return data
        if isinstance(data, dict):
            if "__and__" in data:
                processed_items = [_flatten_logical_expressions_helper(item) for item in data["__and__"]]
                try:
                    return reduce(lambda a, b: a & b, processed_items)
                except TypeError:
                    return f"({' & '.join(str(item) for item in processed_items)})"
            if "__or__" in data:
                processed_items = [_flatten_logical_expressions_helper(item) for item in data["__or__"]]
                try:
                    return reduce(lambda a, b: a | b, processed_items)
                except TypeError:
                    return f"({' | '.join(str(item) for item in processed_items)})"
            if "__join__" in data:
                processed_items = [_flatten_logical_expressions_helper(item) for item in data["__join__"]]
                return "".join(str(item) for item in processed_items)
            new_dict = {}
            for key, value in data.items():
                new_dict[key] = _flatten_logical_expressions_helper(value)
            return new_dict
        if isinstance(data, list):
            return [_flatten_logical_expressions_helper(item) for item in data]
        return data

    def _flatten_logical_expressions(data):
        return _flatten_logical_expressions_helper(copy.deepcopy(data))

    with open(file_path, "r", encoding="utf-8") as fp:
        config_with_env = os.path.expandvars(fp.read())
        config: dict[str, any] = yaml.load(stream=config_with_env, Loader=yaml.FullLoader)
        config = cast_with_type(config)
        config = _flatten_logical_expressions(config)

    return config
