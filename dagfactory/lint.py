"""Config checking behind ``dagfactory lint``.

Each DAG's config is resolved the way the runtime resolves it — the external
``defaults.yml`` chain, the file's own ``default:`` block, and the same
``DagBuilder.get_dag_params`` the builder calls — and then handed to
:func:`dagfactory.parameters.check`. ``DagBuilder.build`` checks every config
against that same function, so what lints clean builds without complaint.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from packaging.version import Version

from dagfactory import parameters
from dagfactory._yaml import load_yaml_file
from dagfactory.dagbuilder import DagBuilder
from dagfactory.dagfactory import SYSTEM_PARAMS, _DagFactory


@dataclass
class Finding:
    """One problem found in a DAG config."""

    file: Path
    dag_id: Optional[str]
    severity: str
    message: str
    path: str = ""

    def render(self) -> str:
        where = self.dag_id or "<file>"
        if self.path:
            where = f"{where}.{self.path}"
        return f"[{where}] {self.message}"


@dataclass
class FileResult:
    """Everything found in one YAML file."""

    file: Path
    findings: List[Finding] = field(default_factory=list)

    @property
    def errors(self) -> List[Finding]:
        return [f for f in self.findings if f.severity == parameters.ERROR]

    @property
    def warnings(self) -> List[Finding]:
        return [f for f in self.findings if f.severity == parameters.WARNING]


class _LintDagBuilder(DagBuilder):
    """A real DagBuilder with only ``build`` stubbed out.

    Subclassing rather than re-implementing means lint resolves each DAG's
    config through ``get_dag_params``, the very method the runtime uses, so
    the two cannot merge defaults differently.
    """

    def build(self) -> Dict[str, Any]:
        return {"dag_id": self.dag_name, "dag": None}


class _LintDagFactory(_DagFactory):
    """A factory that resolves configs without registering or rendering DAGs."""

    def _generate_dags(self, globals):  # noqa: A002 — matches the parent signature
        return

    @staticmethod
    def _serialise_config_md(dag_name, dag_config, default_config):
        return ""


@contextmanager
def _capture_builders() -> Iterator[List[_LintDagBuilder]]:
    """Swap DagBuilder out so every DAG the factory would build is captured."""
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


def lint_file(
    yaml_file: Path,
    airflow_version: Version,
    defaults_config_path: Optional[str] = None,
    check_operators: bool = True,
) -> FileResult:
    """Resolve every DAG in *yaml_file* and check it against the metadata.

    *check_operators* imports each task's operator to confirm it exists. Turn
    it off when linting somewhere the providers are not installed.
    """
    result = FileResult(file=yaml_file)

    # A YAML file that defines no DAGs is not a dag-factory config — a fragment,
    # a values file, someone else's YAML. There is nothing to check against the
    # parameter metadata, and flagging it would be noise.
    try:
        if not is_dag_config(load_yaml_file(str(yaml_file))):
            return result
    except Exception as exc:
        result.findings.append(Finding(yaml_file, None, parameters.ERROR, f"{type(exc).__name__}: {exc}"))
        return result

    kwargs: Dict[str, Any] = {"config_filepath": str(yaml_file.resolve())}
    if defaults_config_path:
        kwargs["defaults_config_path"] = defaults_config_path

    with _capture_builders() as builders:
        try:
            _LintDagFactory(**kwargs).build_dags()
        except Exception as exc:
            result.findings.append(Finding(yaml_file, None, parameters.ERROR, f"{type(exc).__name__}: {exc}"))
            return result

    for builder in builders:
        try:
            config = builder.resolved_params()
        except Exception as exc:
            result.findings.append(
                Finding(
                    yaml_file,
                    builder.dag_name,
                    parameters.ERROR,
                    f"Failed to resolve config: {type(exc).__name__}: {exc}",
                )
            )
            continue
        for severity, path, message in parameters.check_for_lint(
            config, airflow_version, check_operators=check_operators
        ):
            result.findings.append(Finding(yaml_file, builder.dag_name, severity, message, path))

    return result


def is_dag_config(config: Any) -> bool:
    """True if a parsed YAML document looks like it defines DAGs."""
    if not isinstance(config, dict):
        return False
    entries = {k: v for k, v in config.items() if k not in SYSTEM_PARAMS}
    return any(isinstance(v, dict) for v in entries.values())
