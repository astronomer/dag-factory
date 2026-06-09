"""Validator for dagfactory lint command."""

from __future__ import annotations

import ast
import datetime
import importlib.util
import json
import os
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

import yaml
from jsonschema import Draft202012Validator, ValidationError, validators
from packaging.version import InvalidVersion, Version

from dagfactory._yaml import load_yaml_file
from dagfactory.constants import DEFAULTS_FILE_NAMES
from dagfactory.dagfactory import SYSTEM_PARAMS, _DagFactory
from dagfactory.utils import cast_with_type, merge_configs

SCHEMA_PATH = Path(__file__).parent / "schemas" / "dag_parameters.json"


@dataclass
class ValidationIssue:
    """A single validation finding for a DAG inside a YAML file."""

    file: Path
    dag_id: Optional[str]
    severity: str  # "error" or "warning"
    message: str
    path: str = ""

    def render(self) -> str:
        location = f"{self.dag_id}" if self.dag_id else "<file>"
        if self.path:
            location = f"{location}.{self.path}"
        return f"[{location}] {self.message}"


@dataclass
class FileValidationResult:
    """Aggregated validation findings for one YAML file."""

    file: Path
    issues: List[ValidationIssue] = field(default_factory=list)

    @property
    def errors(self) -> List[ValidationIssue]:
        return [i for i in self.issues if i.severity == "error"]

    @property
    def warnings(self) -> List[ValidationIssue]:
        return [i for i in self.issues if i.severity == "warning"]


def load_schema() -> Dict[str, Any]:
    """Load the bundled DAG parameter JSON schema."""
    with SCHEMA_PATH.open("r", encoding="utf-8") as fp:
        return json.load(fp)


def _make_result(file_path: Path, severity: str, message: str) -> FileValidationResult:
    """Build a FileValidationResult holding a single ValidationIssue."""
    r = FileValidationResult(file=file_path)
    r.issues.append(ValidationIssue(file=file_path, dag_id=None, severity=severity, message=message))
    return r


# ---------------------------------------------------------------------------
# JSON Schema validator extension
# ---------------------------------------------------------------------------
def _is_string_or_date(checker, instance):
    return isinstance(instance, (str, datetime.date, datetime.datetime))


def _is_object_or_python_instance(checker, instance):
    """Match a JSON object (dict) OR any non-JSON-primitive Python instance.

    dag-factory's function cast_with_type materialises __type__ directives
    in YAML into real Python objects (e.g. CronTriggerTimetable,
    Dataset, timedelta, callables), so we widen "object" to
    match dicts AND any Python value that isn't a JSON primitive.
    """
    if isinstance(instance, bool):
        return False
    if isinstance(instance, (str, int, float, list, type(None))):
        return False
    return True


def _x_airflow_min_version(validator, value, instance, schema):
    actual = validator.airflow_version
    bound = str(value).strip()
    in_range = actual.major >= int(bound) if "." not in bound else actual >= Version(bound)
    if not in_range:
        yield ValidationError(
            f"was introduced in Airflow {value} and is not valid for the configured "
            f"Airflow {actual}."
        )


def _x_airflow_max_version(validator, value, instance, schema):
    actual = validator.airflow_version
    bound = str(value).strip()
    bound_v = Version(bound)
    if "." not in bound:
        in_range = actual.major <= bound_v.major
    elif bound_v.micro == 0 and bound.count(".") == 1:
        in_range = (actual.major, actual.minor) <= (bound_v.major, bound_v.minor)
    else:
        in_range = actual <= bound_v
    if not in_range:
        yield ValidationError(
            f"is not supported past Airflow {value} and was removed before the configured "
            f"Airflow {actual}."
        )


def _x_deprecated_since(validator, value, instance, schema):
    if validator.airflow_version >= Version(value):
        yield ValidationError(f"is deprecated as of Airflow {value}.")


def _x_dagfactory_supported(validator, value, instance, schema):
    if value is False:
        yield ValidationError(
            "is a valid Airflow DAG argument but is not currently wired through dag-factory; "
            "the value will be silently ignored at runtime "
            "(see github.com/astronomer/dag-factory/issues/696)."
        )


def _x_mutually_exclusive(validator, value, instance, schema):
    if not isinstance(instance, dict):
        return
    for group in value or []:
        fields = group.get("fields", [])
        present = [f for f in fields if instance.get(f) is not None]
        if len(present) > 1:
            base = group.get("message") or "Mutually exclusive fields set together."
            yield ValidationError(f"{base} (set: {', '.join(present)})")


def _x_required_anywhere(validator, value, instance, schema):
    if not isinstance(instance, dict):
        return
    for group in value or []:
        fields = group.get("fields", [])
        if not any(_path_present(instance, f) for f in fields):
            base = group.get("message") or f"At least one of these fields must be set: {', '.join(fields)}."
            yield ValidationError(base)


def _path_present(config: Dict[str, Any], dotted_path: str) -> bool:
    """Return True if *dotted_path* (e.g. ``default_args.start_date``) resolves to a non-None value."""
    current: Any = config
    for part in dotted_path.split("."):
        if not isinstance(current, dict) or part not in current:
            return False
        current = current[part]
    return current is not None


# Extend the validator with custom keywords for the DAG Factory schema annotations.
# We have to allow date and time type values as strings since dag-factory uses yaml.FullLoader,
# which enriches the value types. If we want to avoid this, we will have to use a loader function
# different from the one used by dag-factory, which might also not be ideal.
_LINT_TYPE_CHECKER = (
    Draft202012Validator.TYPE_CHECKER
    .redefine("string", _is_string_or_date)
    .redefine("object", _is_object_or_python_instance)
)
_LintValidatorClass = validators.extend(
    Draft202012Validator,
    type_checker=_LINT_TYPE_CHECKER,
    validators={
        "x-airflow-min-version": _x_airflow_min_version,
        "x-airflow-max-version": _x_airflow_max_version,
        "x-deprecated-since": _x_deprecated_since,
        "x-dagfactory-supported": _x_dagfactory_supported,
        "x-mutually-exclusive": _x_mutually_exclusive,
        "x-required-anywhere": _x_required_anywhere,
    },
)

# Keywords whose violations should be surfaced as warnings rather than errors.
# TODO: x-dagfactory-supported should be a temporary field and can be removed once issue #696 is resolved.
_WARNING_KEYWORDS = {"x-deprecated-since", "x-dagfactory-supported"}


def imports_dagfactory(py_file_path: Path) -> bool:
    """Return True if *py_file_path* imports anything from the ``dagfactory`` package.

    Quick AST-only check — never executes user code. Used by lint to decide
    whether a .py file is a dag-factory loader before attempting to import it.
    """
    try:
        source = py_file_path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return False
    try:
        tree = ast.parse(source, filename=str(py_file_path))
    except SyntaxError:
        return False
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if (node.module or "").split(".")[0] == "dagfactory":
                return True
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.split(".")[0] == "dagfactory":
                    return True
    return False


# ---------------------------------------------------------------------------
# Loader interception (schema-only mode)
# ---------------------------------------------------------------------------
class _LintDagFactory(_DagFactory):
    """_DagFactory subclass that skips DAG generation and markdown serialization."""

    def _generate_dags(self, globals):  # noqa: A002 (matches parent signature)
        return

    @staticmethod
    def _serialise_config_md(dag_name, dag_config, default_config):
        return ""


class _LintDagBuilder:
    """DagBuilder stub that captures merged configs without building Airflow DAGs."""

    def __init__(self, dag_name, dag_config, default_config, yml_dag=None):
        self.dag_name = dag_name
        self.dag_config = deepcopy(dag_config)
        self.default_config = deepcopy(default_config)

    def build(self):
        # build_dags returns {"dag_id": ..., "dag": ...}; the dag value is
        # discarded (validators don't need a real Airflow DAG object).
        return {"dag_id": self.dag_name, "dag": None}


@contextmanager
def _intercept_dag_factory() -> Iterator[List[_LintDagFactory]]:
    """Swap _DagFactory in its module; yield a list that captures every instantiation."""
    import dagfactory.dagfactory as df_module

    captured: List[_LintDagFactory] = []

    class _Capturing(_LintDagFactory):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            captured.append(self)

    original = df_module._DagFactory
    df_module._DagFactory = _Capturing
    try:
        yield captured
    finally:
        df_module._DagFactory = original


@contextmanager
def _intercept_dag_builder() -> Iterator[List[_LintDagBuilder]]:
    """Swap DagBuilder in its module; yield a list that captures every instantiation."""
    import dagfactory.dagfactory as df_module

    captured: List[_LintDagBuilder] = []

    class _Capturing(_LintDagBuilder):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            captured.append(self)

    original = df_module.DagBuilder
    df_module.DagBuilder = _Capturing
    try:
        yield captured
    finally:
        df_module.DagBuilder = original


def _import_loader(py_path: Path) -> Tuple[Optional[Any], Optional[Exception]]:
    """Import *py_path* as a Python module. Returns ``(module, error_or_None)``."""
    spec = importlib.util.spec_from_file_location("_dagfactory_lint_module", py_path)
    if spec is None or spec.loader is None:
        return None, ImportError(f"Could not load Python module from {py_path}.")
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except Exception as exc:
        return None, exc
    return module, None


# ---------------------------------------------------------------------------
# Validator
# ---------------------------------------------------------------------------
class DagParameterValidator:
    """Validate DAG Factory YAML files against the bundled JSON schema."""

    def __init__(
        self,
        airflow_version: str = "3",
        schema: Optional[Dict[str, Any]] = None,
        schema_only: bool = True,
    ) -> None:
        try:
            self.airflow_version = Version(str(airflow_version))
        except InvalidVersion as exc:
            raise ValueError(f"airflow_version must be a PEP440 version string, got: {airflow_version!r}") from exc

        self.schema_only = schema_only
        self.schema = schema if schema is not None else load_schema()
        # Subclass the extended validator class so we can attach airflow_version
        # as a class attribute (instances are slotted and reject arbitrary
        # attribute assignment). The keyword functions read it off the validator.
        validator_cls = type(
            "_DagParameterValidator",
            (_LintValidatorClass,),
            {"airflow_version": self.airflow_version},
        )
        self._validator = validator_cls(self.schema)

    # ------------------------------------------------------------------
    # Entry points
    # ------------------------------------------------------------------
    def validate_python_loader(self, py_file_path: Path) -> List[FileValidationResult]:
        """Lint a Python loader file (one that calls ``load_yaml_dags``).

        * schema_only=True → intercept dag-factory, capture merged configs,
          validate against the JSON schema.
        * schema_only=False → let dag-factory + Airflow build real DAGs;
          surface any exception as a ValidationIssue.
        """
        py_file_path = py_file_path.resolve()
        if not imports_dagfactory(py_file_path):
            return [_make_result(py_file_path, "warning", "File does not import dagfactory; nothing to lint.")]

        if not self.schema_only:
            # Build mode: import the loader untouched, capture exceptions.
            result = FileValidationResult(file=py_file_path)
            module, exc = _import_loader(py_file_path)
            if exc is not None:
                result.issues.append(
                    ValidationIssue(
                        file=py_file_path, dag_id=None, severity="error",
                        message=f"Failed to build DAGs: {type(exc).__name__}: {exc}",
                    )
                )
                return [result]
            if not self._find_dag_objects(module):
                result.issues.append(
                    ValidationIssue(
                        file=py_file_path, dag_id=None, severity="warning",
                        message="Loader imported successfully but no DAGs were built.",
                    )
                )
            return [result]

        # Schema mode: capture _DagFactory instantiations during import,
        # then replay each through DagBuilder intercept to grab merged configs.
        with _intercept_dag_factory() as factories:
            _, exc = _import_loader(py_file_path)
        if exc is not None:
            return [_make_result(
                py_file_path, "error",
                f"Failed to import {py_file_path.name}: {type(exc).__name__}: {exc}",
            )]
        if not factories:
            return [_make_result(
                py_file_path, "warning",
                "No dagfactory loader was invoked when importing this file.",
            )]

        results: List[FileValidationResult] = []
        for factory in factories:
            target = Path(factory.config_file_path) if factory.config_file_path else py_file_path
            result = FileValidationResult(file=target)
            with _intercept_dag_builder() as builders:
                try:
                    factory.build_dags()
                except Exception as exc:
                    # build_dags can raise from malformed configs.
                    result.issues.append(
                        ValidationIssue(
                            file=target, dag_id=None, severity="error",
                            message=f"dag-factory failed to build DAGs: {type(exc).__name__}: {exc}",
                        )
                    )
            for builder in builders:
                merged = merge_configs(builder.dag_config, builder.default_config)
                self._validate_dag(target, builder.dag_name, merged, result)
            results.append(result)
        return results

    def validate_yaml_file(self, yaml_file_path: Path) -> List[FileValidationResult]:
        """Lint a YAML file as a complete DAG config.

        The YAML's own top-level ``default`` block IS applied. External
        defaults sources (``defaults.yml`` chain, ``defaults_config_dict``,
        ``defaults_config_path``) are NOT consulted.
        Files named ``defaults.yml`` / ``defaults.yaml`` are skipped.
        """
        yaml_file_path = yaml_file_path.resolve()
        if yaml_file_path.name in DEFAULTS_FILE_NAMES:
            return [_make_result(
                yaml_file_path, "warning",
                f"Skipping {yaml_file_path.name} — defaults file, not a DAG config.",
            )]
        try:
            config = load_yaml_file(str(yaml_file_path))
        except Exception as exc:
            return [_make_result(
                yaml_file_path, "error",
                f"Failed to load YAML: {type(exc).__name__}: {exc}",
            )]
        return [self._validate_yaml_config(config, yaml_file_path)]

    def validate_yaml_content(
        self, yaml_content: str, source_label: str = "<inline yaml>"
    ) -> List[FileValidationResult]:
        """Lint an inline YAML string. ``__and__``/``__or__``/``__join__`` directives are not expanded."""
        source = Path(source_label)
        try:
            raw = yaml.load(os.path.expandvars(yaml_content), Loader=yaml.FullLoader)
            config = cast_with_type(raw)
        except Exception as exc:
            return [_make_result(
                source, "error",
                f"Failed to parse YAML: {type(exc).__name__}: {exc}",
            )]
        return [self._validate_yaml_config(config, source)]

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------
    def _validate_yaml_config(self, config: Any, source: Path) -> FileValidationResult:
        """Validate a parsed YAML dict; routes through schema or build mode."""
        result = FileValidationResult(file=source)

        if not isinstance(config, dict):
            result.issues.append(ValidationIssue(
                file=source, dag_id=None, severity="error",
                message="Top-level YAML must be a mapping.",
            ))
            return result

        dag_entries = {k: v for k, v in config.items() if k not in SYSTEM_PARAMS}
        if not dag_entries:
            result.issues.append(ValidationIssue(
                file=source, dag_id=None, severity="warning",
                message="No DAG entries found in YAML (only reserved top-level keys found).",
            ))
            return result
        # Non-DAG YAML (top-level values aren't mappings) — skip with one warning.
        if not any(isinstance(v, dict) for v in dag_entries.values()):
            result.issues.append(ValidationIssue(
                file=source, dag_id=None, severity="warning",
                message="No DAG entries found (top-level values are not mappings); "
                "looks like a config-only YAML, skipping.",
            ))
            return result

        if not self.schema_only:
            try:
                _DagFactory(config_dict=config).build_dags()
            except Exception as exc:
                result.issues.append(ValidationIssue(
                    file=source, dag_id=None, severity="error",
                    message=f"Failed to build DAGs: {type(exc).__name__}: {exc}",
                ))
            return result

        # Schema mode: build via lint factory under DagBuilder intercept,
        # then validate each captured config against the JSON schema.
        with _intercept_dag_builder() as builders:
            try:
                _LintDagFactory(config_dict=config).build_dags()
            except Exception as exc:
                result.issues.append(ValidationIssue(
                    file=source, dag_id=None, severity="error",
                    message=f"dag-factory failed to process YAML: {type(exc).__name__}: {exc}",
                ))
        for builder in builders:
            merged = merge_configs(builder.dag_config, builder.default_config)
            self._validate_dag(source, builder.dag_name, merged, result)
        return result

    @staticmethod
    def _find_dag_objects(module) -> Dict[str, object]:
        """Collect any Airflow DAG instances registered in the module globals."""
        try:
            from airflow.sdk.definitions.dag import DAG
        except ImportError:
            from airflow.models import DAG
        return {name: obj for name, obj in vars(module).items() if isinstance(obj, DAG)}

    def _validate_dag(
        self,
        file_path: Path,
        dag_id: str,
        merged: Dict[str, Any],
        result: FileValidationResult,
    ) -> None:
        """Run the JSON schema over a merged DAG config and append issues to *result*."""
        for error in self._validator.iter_errors(merged):
            severity = "warning" if error.validator in _WARNING_KEYWORDS else "error"
            result.issues.append(
                ValidationIssue(
                    file=file_path, dag_id=dag_id, severity=severity, message=error.message,
                    path=".".join(str(p) for p in error.absolute_path),
                )
            )
