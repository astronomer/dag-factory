"""Tests for dagfactory.validator: imports_dagfactory helper + DagParameterValidator."""
import sys
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
# _import_loader — module registration
# ---------------------------------------------------------------------------
def test_import_loader_registers_module_in_sys_modules(tmp_path):
    """A loader that resolves its own forward-referenced type hints (e.g. a
    dataclass field typed with another class in the same file, under
    `from __future__ import annotations`) needs the module registered in
    sys.modules — otherwise resolving the annotation raises."""
    p = _write_loader(
        tmp_path,
        """
        from __future__ import annotations
        import typing
        from dataclasses import dataclass
        from dagfactory import load_yaml_dags

        @dataclass
        class Inner:
            pass

        @dataclass
        class Config:
            inner: Inner

        typing.get_type_hints(Config)

        load_yaml_dags(
            globals_dict=globals(),
            config_dict={"my_dag": {"tasks": [{"task_id": "t", "operator": "x"}]}},
            defaults_config_dict={"default_args": {"start_date": "2025-01-01"}},
        )
        """,
    )
    results = DagParameterValidator(schema_only=True).validate_python_loader(p)
    assert not results[0].errors


def test_import_loader_uses_unique_module_name_per_file(tmp_path):
    """Two files linted in the same process must not clobber each other's
    sys.modules entry (they used to share one hardcoded module name)."""
    first = _write_loader(
        tmp_path,
        """
        from dagfactory import load_yaml_dags
        MARKER = "first"
        load_yaml_dags(
            globals_dict=globals(),
            config_dict={"first_dag": {"tasks": [{"task_id": "t", "operator": "x"}]}},
            defaults_config_dict={"default_args": {"start_date": "2025-01-01"}},
        )
        """,
        name="first.py",
    )
    second = _write_loader(
        tmp_path,
        """
        from dagfactory import load_yaml_dags
        MARKER = "second"
        load_yaml_dags(
            globals_dict=globals(),
            config_dict={"second_dag": {"tasks": [{"task_id": "t", "operator": "x"}]}},
            defaults_config_dict={"default_args": {"start_date": "2025-01-01"}},
        )
        """,
        name="second.py",
    )
    validator = DagParameterValidator(schema_only=True)
    first_results = validator.validate_python_loader(first)
    second_results = validator.validate_python_loader(second)
    assert not first_results[0].errors
    assert not second_results[0].errors
    assert not any(m.startswith("_dagfactory_lint_module") for m in sys.modules)


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


def test_validate_yaml_file_missing_start_date_is_a_warning(tmp_path):
    """validate_yaml_file never resolves the external defaults.yml chain, so a
    DAG that gets `start_date` from there (a common pattern) shouldn't be a
    hard error here — just a warning flagging the gap."""
    p = tmp_path / "dag.yml"
    p.write_text("my_dag:\n  tasks:\n    - task_id: t\n      operator: x\n")
    results = DagParameterValidator(airflow_version="3").validate_yaml_file(p)
    assert not results[0].errors
    assert any("start_date" in w.message for w in results[0].warnings)


def test_validate_yaml_file_missing_tasks_is_a_warning(tmp_path):
    """Same as above for a `tasks` list supplied only via external defaults."""
    p = tmp_path / "dag.yml"
    p.write_text("my_dag:\n  default_args:\n    start_date: '2025-01-01'\n")
    results = DagParameterValidator(airflow_version="3").validate_yaml_file(p)
    assert not results[0].errors
    assert any("tasks" in w.message for w in results[0].warnings)


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


def test_validate_yaml_content_expands_join_directive():
    """--yaml-content shares load_yaml_string with the file path, so __join__/
    __and__/__or__ directives are flattened the same way dag-factory actually
    builds them, not left as raw dicts that would fail schema validation."""
    yaml_text = (
        "my_dag:\n"
        "  default_args:\n"
        "    start_date: '2025-01-01'\n"
        "    owner:\n"
        "      __join__: ['team-', 'data']\n"
        "  tasks:\n"
        "    - task_id: t\n"
        "      operator: x\n"
    )
    results = DagParameterValidator(airflow_version="3").validate_yaml_content(yaml_text)
    assert not results[0].errors


# ---------------------------------------------------------------------------
# Custom x-* JSON Schema keywords
# ---------------------------------------------------------------------------
def test_x_deprecated_since_is_a_warning_not_error():
    """x-deprecated-since (schedule_interval) is a warning once the configured
    Airflow version reaches the deprecation, not an error."""
    yaml_text = (
        "my_dag:\n"
        "  schedule_interval: '@daily'\n"
        "  default_args:\n"
        "    start_date: '2025-01-01'\n"
        "  tasks:\n"
        "    - task_id: t\n"
        "      operator: x\n"
    )
    results = DagParameterValidator(airflow_version="2.4").validate_yaml_content(yaml_text)
    assert not results[0].errors
    assert any("deprecated" in w.message for w in results[0].warnings)


def test_x_dagfactory_supported_false_is_a_warning_not_error():
    """x-dagfactory-supported: false (template_undefined) is a warning — the
    value is valid Airflow but silently ignored by dag-factory at runtime."""
    yaml_text = (
        "my_dag:\n"
        "  template_undefined: 'AllowUndefined'\n"
        "  default_args:\n"
        "    start_date: '2025-01-01'\n"
        "  tasks:\n"
        "    - task_id: t\n"
        "      operator: x\n"
    )
    results = DagParameterValidator(airflow_version="3").validate_yaml_content(yaml_text)
    assert not results[0].errors
    assert any("not currently wired through dag-factory" in w.message for w in results[0].warnings)


def test_x_mutually_exclusive_is_an_error():
    """x-mutually-exclusive (schedule vs schedule_interval vs timetable) errors
    when more than one of the group is set."""
    yaml_text = (
        "my_dag:\n"
        "  schedule: '@daily'\n"
        "  schedule_interval: '@daily'\n"
        "  default_args:\n"
        "    start_date: '2025-01-01'\n"
        "  tasks:\n"
        "    - task_id: t\n"
        "      operator: x\n"
    )
    results = DagParameterValidator(airflow_version="2").validate_yaml_content(yaml_text)
    assert any("Only one of" in e.message for e in results[0].errors)


def test_x_airflow_min_version_is_an_error():
    """x-airflow-min-version (allowed_run_types, introduced in 3.2) errors when
    the configured Airflow predates it."""
    yaml_text = (
        "my_dag:\n"
        "  allowed_run_types: ['backfill']\n"
        "  default_args:\n"
        "    start_date: '2025-01-01'\n"
        "  tasks:\n"
        "    - task_id: t\n"
        "      operator: x\n"
    )
    results = DagParameterValidator(airflow_version="3.0").validate_yaml_content(yaml_text)
    assert any("was introduced in Airflow" in e.message for e in results[0].errors)


def test_x_required_anywhere_is_an_error_when_defaults_are_fully_resolved(tmp_path):
    """Unlike validate_yaml_content/validate_yaml_file (which soften this to a
    warning — see test_validate_yaml_file_missing_start_date_is_a_warning),
    validate_python_loader resolves the real defaults chain, so if start_date
    is genuinely missing everywhere it's still a hard error."""
    p = _write_loader(
        tmp_path,
        """
        from dagfactory import load_yaml_dags
        load_yaml_dags(
            globals_dict=globals(),
            config_dict={"my_dag": {"tasks": [{"task_id": "t", "operator": "x"}]}},
        )
        """,
    )
    results = DagParameterValidator(schema_only=True).validate_python_loader(p)
    rendered = " ".join(i.render() for i in results[0].errors)
    assert "start_date" in rendered
