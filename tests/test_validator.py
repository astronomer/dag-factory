"""Tests for dagfactory.validator: imports_dagfactory helper + DagParameterValidator."""
import textwrap
from pathlib import Path

import pytest

from dagfactory.validator import DagParameterValidator, imports_dagfactory


def _write_loader(tmp_path: Path, source: str, name: str = "loader.py") -> Path:
    p = tmp_path / name
    p.write_text(textwrap.dedent(source))
    return p


# ---------------------------------------------------------------------------
# imports_dagfactory — AST-based loader detection
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "source",
    [
        "from dagfactory import load_yaml_dags\n",  # ImportFrom
        "import dagfactory\n",  # Import
    ],
)
def test_imports_dagfactory_true(tmp_path, source):
    p = tmp_path / "loader.py"
    p.write_text(source)
    assert imports_dagfactory(p) is True


@pytest.mark.parametrize(
    "source",
    [
        "import os\n",
        # AST-based check — string content of a comment/literal must not match.
        "# from dagfactory import load_yaml_dags\nx = 'dagfactory'\n",
        # Top-level segment match — `dagfactory_other` is not `dagfactory`.
        "from dagfactory_other import foo\n",
    ],
)
def test_imports_dagfactory_false(tmp_path, source):
    p = tmp_path / "non_loader.py"
    p.write_text(source)
    assert imports_dagfactory(p) is False


def test_imports_dagfactory_handles_syntax_error(tmp_path):
    p = tmp_path / "broken.py"
    p.write_text("def f(:\n  pass\n")  # syntax error
    assert imports_dagfactory(p) is False


def test_imports_dagfactory_handles_missing_file(tmp_path):
    assert imports_dagfactory(tmp_path / "nonexistent.py") is False


# ---------------------------------------------------------------------------
# validate_python_loader — build mode (default)
# ---------------------------------------------------------------------------
def test_build_mode_loader_without_dagfactory_import_is_skipped(tmp_path):
    p = _write_loader(tmp_path, "def helper(): return 42\n", name="utils.py")
    results = DagParameterValidator(schema_only=False).validate_python_loader(p)
    assert len(results) == 1
    assert len(results[0].warnings) == 1
    assert "does not import dagfactory" in results[0].warnings[0].message


def test_build_mode_loader_imports_but_does_not_call_load_yaml_dags(tmp_path):
    p = _write_loader(tmp_path, "import dagfactory  # but no load_yaml_dags call\n")
    results = DagParameterValidator(schema_only=False).validate_python_loader(p)
    assert len(results) == 1
    assert len(results[0].warnings) == 1
    assert "no DAGs were built" in results[0].warnings[0].message


def test_build_mode_dependency_cycle_is_caught(tmp_path):
    """Exception during dag-factory build (here: cycle detection) surfaces as a single error.

    This covers the generic exception-capture path in `_import_loader` — bad
    operator paths and missing-module imports hit the same code path.
    """
    p = _write_loader(
        tmp_path,
        """
        from dagfactory import load_yaml_dags
        load_yaml_dags(
            globals_dict=globals(),
            config_dict={"cyclic_dag": {"tasks": [
                {"task_id": "a", "operator": "airflow.operators.bash.BashOperator",
                 "bash_command": "echo a", "dependencies": ["b"]},
                {"task_id": "b", "operator": "airflow.operators.bash.BashOperator",
                 "bash_command": "echo b", "dependencies": ["a"]},
            ]}},
            defaults_config_dict={"default_args": {"start_date": "2025-01-01"}},
        )
        """,
    )
    results = DagParameterValidator(schema_only=False).validate_python_loader(p)
    errors = results[0].errors
    assert errors, "expected an error from cycle detection"
    assert "Failed to build DAGs" in errors[0].message
    assert "cycle" in errors[0].message.lower()


# ---------------------------------------------------------------------------
# validate_python_loader — schema mode
# ---------------------------------------------------------------------------
def test_schema_mode_catches_removed_field_on_af3(tmp_path):
    """schema_only=True surfaces version-removal errors without building DAGs."""
    p = _write_loader(
        tmp_path,
        """
        from dagfactory import load_yaml_dags
        load_yaml_dags(
            globals_dict=globals(),
            config_dict={"my_dag": {
                "schedule_interval": "@daily",
                "tasks": [{"task_id": "t",
                    "operator": "airflow.operators.bash.BashOperator",
                    "bash_command": "echo"}]}},
            defaults_config_dict={"default_args": {"start_date": "2025-01-01"}},
        )
        """,
    )
    results = DagParameterValidator(schema_only=True, airflow_version="3.1").validate_python_loader(p)
    rendered = " ".join(i.render() for i in results[0].errors)
    assert "schedule_interval" in rendered
    assert "not supported past Airflow 2" in rendered


def test_schema_mode_config_dict_loader_ignores_sibling_defaults_yml(tmp_path):
    """Regression: when a .py loader uses config_dict (no YAML), lint must NOT
    walk up from the .py file's directory to pick up an unrelated defaults.yml.

    The validator drives ``_DagFactory.build_dags`` directly under a stubbed
    DagBuilder, so this behaviour is inherited from the runtime: a factory
    built from ``config_dict=...`` has ``config_file_path = None`` and
    ``_retrieve_possible_default_config_dirs`` short-circuits the parent walk.
    """
    user_defaults_dir = tmp_path / "config"
    user_defaults_dir.mkdir()
    (user_defaults_dir / "defaults.yml").write_text("tags: [from-user-config-dir]\n")

    loader_dir = tmp_path / "src"
    loader_dir.mkdir()
    # A defaults.yml sitting next to the loader. Runtime would NOT see this
    # in the config_dict case; lint must match.
    (loader_dir / "defaults.yml").write_text("tags: [from-sibling-loader-dir]\n")

    loader = loader_dir / "loader.py"
    loader.write_text(
        textwrap.dedent(
            f"""
            from dagfactory import load_yaml_dags
            load_yaml_dags(
                globals_dict=globals(),
                config_dict={{"my_dag": {{"tasks": [{{"task_id": "t", "operator": "x"}}]}}}},
                defaults_config_path={str(user_defaults_dir)!r},
            )
            """
        )
    )

    results = DagParameterValidator(airflow_version="3").validate_python_loader(loader)
    assert len(results) == 1
    # The sibling defaults.yml's tag value would only show up if lint had
    # walked the loader's parent dir — which it must not.
    assert not any("from-sibling-loader-dir" in i.message for r in results for i in r.issues)


# ---------------------------------------------------------------------------
# validate_yaml_file
# ---------------------------------------------------------------------------
def test_validate_yaml_file_catches_removed_field(tmp_path):
    """`schedule_interval` in YAML triggers the schema's removed-in-AF3 error."""
    p = tmp_path / "dag.yml"
    p.write_text(
        "my_dag:\n"
        "  schedule_interval: '@daily'\n"
        "  default_args:\n"
        "    start_date: '2025-01-01'\n"
        "  tasks:\n"
        "    - task_id: t\n"
        "      operator: x\n"
    )
    results = DagParameterValidator(airflow_version="3.1").validate_yaml_file(p)
    rendered = " ".join(i.render() for i in results[0].errors)
    assert "schedule_interval" in rendered


def test_validate_yaml_file_applies_internal_default_block(tmp_path):
    """YAML mode DOES merge the YAML's own `default` block (it's part of the same file)."""
    p = tmp_path / "dag.yml"
    p.write_text(
        "default:\n"
        "  default_args:\n"
        "    start_date: '2025-01-01'\n"
        "my_dag:\n"
        "  tasks:\n"
        "    - task_id: t\n"
        "      operator: x\n"
    )
    results = DagParameterValidator(airflow_version="3").validate_yaml_file(p)
    assert not results[0].errors


def test_validate_yaml_file_skips_defaults_yml(tmp_path):
    """defaults.yml files are dag-factory infrastructure, not standalone DAGs."""
    p = tmp_path / "defaults.yml"
    p.write_text("default_args:\n  start_date: '2025-01-01'\n  owner: alice\n")
    results = DagParameterValidator(airflow_version="3").validate_yaml_file(p)
    assert not results[0].errors
    assert any("defaults file" in i.message for i in results[0].warnings)


def test_validate_yaml_file_skips_non_dag_yaml(tmp_path):
    """A YAML with no DAG-shaped entries (e.g. dataset config list) is skipped."""
    p = tmp_path / "datasets.yml"
    p.write_text("datasets:\n  - name: d1\n    uri: s3://x\n")
    results = DagParameterValidator(airflow_version="3").validate_yaml_file(p)
    assert not results[0].errors
    assert any("config-only" in i.message for i in results[0].warnings)


# ---------------------------------------------------------------------------
# validate_yaml_content
# ---------------------------------------------------------------------------
def test_validate_yaml_content_catches_schema_error():
    yaml_text = (
        "my_dag:\n"
        "  catchup: 'yes'\n"  # not a boolean
        "  default_args:\n"
        "    start_date: '2025-01-01'\n"
        "  tasks:\n"
        "    - task_id: t\n"
        "      operator: x\n"
    )
    results = DagParameterValidator(airflow_version="3").validate_yaml_content(yaml_text)
    rendered = " ".join(i.render() for i in results[0].errors)
    assert "catchup" in rendered and "boolean" in rendered


def test_validate_yaml_content_with_label():
    yaml_text = "my_dag:\n  default_args: {start_date: '2025-01-01'}\n  tasks: [{task_id: t, operator: x}]\n"
    results = DagParameterValidator(airflow_version="3").validate_yaml_content(
        yaml_text, source_label="editor:buffer.yml"
    )
    assert results[0].file == Path("editor:buffer.yml")


def test_validate_yaml_content_parse_error():
    results = DagParameterValidator(airflow_version="3").validate_yaml_content("key: [unclosed\n")
    assert results[0].errors
    assert "Failed to parse YAML" in results[0].errors[0].message
