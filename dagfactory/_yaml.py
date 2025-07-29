from __future__ import annotations

import os

import yaml

from .utils import cast_with_type


def load_yaml_file(file_path: str) -> dict[str, any]:
    """
    Load a YAML file into a dictionary.
    """

    def __join(loader: yaml.FullLoader, node: yaml.Node) -> str:
        seq = loader.construct_sequence(node)
        return "".join([str(i) for i in seq])

    def __or(loader: yaml.FullLoader, node: yaml.Node) -> str:
        seq = loader.construct_sequence(node)
        return " | ".join([f"({str(i)})" for i in seq])

    def __and(loader: yaml.FullLoader, node: yaml.Node) -> str:
        seq = loader.construct_sequence(node)
        return " & ".join([f"({str(i)})" for i in seq])

    yaml.add_constructor("!join", __join, yaml.FullLoader)
    yaml.add_constructor("!or", __or, yaml.FullLoader)
    yaml.add_constructor("!and", __and, yaml.FullLoader)

    with open(file_path, "r", encoding="utf-8") as fp:
        config_with_env = os.path.expandvars(fp.read())
        config: dict[str, any] = yaml.load(stream=config_with_env, Loader=yaml.FullLoader)
        config = cast_with_type(config)

    return config
