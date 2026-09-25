"""Canonical metadata for every configuration key dag-factory understands.

One plain dict. :mod:`dagfactory.dagbuilder` reads it to decide which keys
reach the ``DAG`` constructor, which Airflow versions accept them, and which
are deprecated aliases. Stating a fact once is the point: a version bound
written here changes what gets forwarded and what gets reported, together.

Each entry maps a configuration key to a metadata mapping. Recognised fields:

``types``
    Tuple of Python types the value may take. Checked with ``isinstance``.
``enum``
    Tuple of permitted values.
``minimum``
    Smallest permitted number.
``pattern``
    Regular expression the whole value must match.
``min_version`` / ``max_version``
    The half-open Airflow range ``[min_version, max_version)`` in which the key
    applies, as PEP 440 strings. The upper bound is exclusive, so ``"3.0.0"``
    reads as "gone in 3.0" with no ambiguity about how many components were
    written.
``deprecated_since``
    Airflow version that deprecated the key.
``deprecated_in_favor_of``
    The key that supersedes this one. dag-factory rewrites the value to that
    key, so the alias can outlive the Airflow argument.
``supported``
    ``False`` for a valid Airflow argument dag-factory does not forward.
``dagfactory_only``
    ``True`` for dag-factory's own keys, which are never forwarded to Airflow.
``handled``
    ``True`` when dag-factory interprets the key itself rather than forwarding
    it (``schedule`` and friends).
``transform``
    Callable applied to the value before it reaches the ``DAG`` constructor.
``scope``
    Where the key may appear: any of ``"dag"``, ``"task"``, ``"default_args"``.
``required``
    The author must set this key; the linter reports its absence.
``description``
    Prose for diagnostics.
"""

from __future__ import annotations

import re
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple

from packaging.version import Version

from dagfactory.utils import resolve_user_defined_macros

#: Issue tracking the DAG arguments dag-factory does not yet forward.
UNSUPPORTED_ARGS_ISSUE = "https://github.com/astronomer/dag-factory/issues/696"

#: Severity levels a finding can carry.
ERROR = "error"
WARNING = "warning"

_FIELDS = frozenset(
    {
        "types",
        "enum",
        "minimum",
        "pattern",
        "min_version",
        "max_version",
        "deprecated_since",
        "deprecated_in_favor_of",
        "supported",
        "dagfactory_only",
        "handled",
        "transform",
        "scope",
        "required",
        "description",
    }
)

CALLBACK = (str, dict, list)
TIMEDELTA_LIKE = (str, int, float, dict)
DAG = ("dag",)
TASK = ("task", "default_args")
DAG_AND_DEFAULTS = ("dag", "default_args")
ANY_LEVEL = ("dag", "task", "default_args")

PARAM_METADATA: Dict[str, Dict[str, Any]] = {
    # ---- DAG-level, forwarded to DAG() -------------------------------------
    "dag_id": {"types": (str,), "pattern": r"^[A-Za-z0-9._-]+$", "scope": DAG},
    "dag_display_name": {"types": (str,), "scope": DAG},
    "description": {"types": (str,), "scope": DAG},
    "catchup": {"types": (bool,), "scope": DAG},
    "max_active_tasks": {"types": (int,), "minimum": 1, "scope": DAG},
    "max_active_runs": {"types": (int,), "minimum": 1, "scope": DAG},
    "concurrency": {
        "types": (int,),
        "deprecated_since": "2.2",
        "deprecated_in_favor_of": "max_active_tasks",
        "scope": DAG,
        # No max_version: dag-factory rewrites this to max_active_tasks, which
        # every supported Airflow accepts, so the YAML key outlives the kwarg.
        "description": "Deprecated alias for max_active_tasks.",
    },
    "dagrun_timeout": {"types": TIMEDELTA_LIKE, "scope": DAG},
    "default_view": {
        "types": (str,),
        "enum": ("grid", "graph", "duration", "gantt", "landing_times", "tree"),
        "max_version": "3.0.0",
        "scope": DAG,
    },
    "orientation": {"types": (str,), "enum": ("LR", "TB", "RL", "BT"), "max_version": "3.0.0", "scope": DAG},
    "template_searchpath": {"types": (str, list), "scope": DAG},
    "render_template_as_native_obj": {"types": (bool,), "scope": DAG},
    "sla_miss_callback": {
        "types": CALLBACK,
        "max_version": "3.1.0",
        "deprecated_since": "2.0",
        "scope": DAG,
    },
    "doc_md": {"types": (str,), "scope": DAG},
    "access_control": {"types": (dict,), "scope": DAG},
    "is_paused_upon_creation": {"types": (bool,), "scope": DAG},
    # Both DAG() and BaseOperator take params, so it is valid anywhere.
    "params": {"types": (dict,), "scope": ANY_LEVEL},
    "user_defined_macros": {
        "types": (dict,),
        "transform": resolve_user_defined_macros,
        "scope": DAG,
        "description": "String values are imported as callables via their dotted path.",
    },
    "default_args": {"types": (dict,), "scope": DAG},
    "timetable": {
        "types": (dict,),
        "max_version": "3.0.0",
        "scope": DAG,
        "description": "Airflow 3 takes a Timetable through `schedule` instead.",
    },
    # ---- DAG-level, interpreted by dag-factory -----------------------------
    "schedule": {"types": (str, int, float, list, dict, type(None)), "handled": True, "scope": DAG},
    "schedule_interval": {
        "types": (str, int, float, dict, type(None)),
        "max_version": "3.0.0",
        "deprecated_since": "2.4",
        "handled": True,
        "scope": DAG,
        "description": "Use `schedule` instead.",
    },
    "tags": {"types": (list,), "handled": True, "scope": DAG},
    # ---- Valid Airflow arguments dag-factory does not forward --------------
    "template_undefined": {"types": (str,), "supported": False, "scope": DAG},
    "user_defined_filters": {"types": (dict,), "supported": False, "scope": DAG},
    "max_consecutive_failed_dag_runs": {
        "types": (int,),
        "minimum": 0,
        "min_version": "2.9.0",
        "supported": False,
        "scope": DAG,
    },
    "auto_register": {"types": (bool,), "min_version": "2.7.0", "supported": False, "scope": DAG},
    "fail_fast": {"types": (bool,), "supported": False, "scope": DAG},
    "owner_links": {"types": (dict,), "min_version": "2.3.0", "supported": False, "scope": DAG},
    "jinja_environment_kwargs": {"types": (dict,), "supported": False, "scope": DAG},
    "allowed_run_types": {"types": (list,), "min_version": "3.2.0", "supported": False, "scope": DAG},
    "deadline": {
        "types": (list, dict, type(None)),
        "min_version": "3.1.0",
        "supported": False,
        "scope": DAG,
        "description": "Replacement for the removed SLA feature.",
    },
    # ---- Valid on the DAG body and in default_args -------------------------
    "start_date": {"types": (str,), "required": True, "scope": DAG_AND_DEFAULTS},
    "end_date": {"types": (str,), "scope": DAG_AND_DEFAULTS},
    "on_success_callback": {"types": CALLBACK, "scope": DAG_AND_DEFAULTS},
    "on_failure_callback": {"types": CALLBACK, "scope": DAG_AND_DEFAULTS},
    # ---- dag-factory's own keys --------------------------------------------
    "tasks": {"types": (dict, list), "dagfactory_only": True, "required": True, "scope": DAG},
    "task_groups": {"types": (dict, list), "dagfactory_only": True, "scope": DAG},
    "timezone": {"types": (str,), "dagfactory_only": True, "scope": DAG_AND_DEFAULTS},
    "doc_md_file_path": {"types": (str,), "dagfactory_only": True, "scope": DAG},
    "doc_md_python_callable_file": {"types": (str,), "dagfactory_only": True, "scope": DAG},
    "doc_md_python_callable_name": {"types": (str,), "dagfactory_only": True, "scope": DAG},
    "doc_md_python_arguments": {"types": (dict,), "dagfactory_only": True, "scope": DAG},
    # ---- Task-level, also accepted in default_args -------------------------
    "owner": {"types": (str,), "scope": ("default_args",)},
    "email": {"types": (str, list), "scope": TASK},
    "email_on_failure": {"types": (bool,), "scope": TASK},
    "email_on_retry": {"types": (bool,), "scope": TASK},
    "retries": {"types": (int,), "minimum": 0, "scope": TASK},
    "retry_delay": {"types": TIMEDELTA_LIKE, "scope": TASK},
    "retry_exponential_backoff": {"types": (bool,), "scope": TASK},
    "max_retry_delay": {"types": TIMEDELTA_LIKE, "scope": TASK},
    "depends_on_past": {"types": (bool,), "scope": TASK},
    "wait_for_downstream": {"types": (bool,), "scope": TASK},
    "queue": {"types": (str,), "scope": TASK},
    "pool": {"types": (str,), "scope": TASK},
    "pool_slots": {"types": (int,), "minimum": 1, "scope": TASK},
    "priority_weight": {"types": (int,), "scope": TASK},
    "weight_rule": {"types": (str,), "scope": TASK},
    "execution_timeout": {"types": TIMEDELTA_LIKE, "scope": TASK},
    "trigger_rule": {"types": (str,), "scope": TASK},
    "sla": {"types": TIMEDELTA_LIKE, "max_version": "3.1.0", "deprecated_since": "2.0", "scope": TASK},
    "on_retry_callback": {"types": CALLBACK, "scope": TASK},
    "on_execute_callback": {"types": CALLBACK, "scope": TASK},
}

#: Keys that may not be set together, with the message to report.
MUTUALLY_EXCLUSIVE: List[Tuple[Tuple[str, ...], str]] = [
    (("schedule", "schedule_interval", "timetable"), "Only one of {fields} may be set."),
    (
        ("doc_md", "doc_md_file_path", "doc_md_python_callable_file"),
        "Pick a single source for `doc_md`: an inline string, a file path, or a python callable.",
    ),
]

#: Keys that require other keys to be set alongside them.
REQUIRES: Dict[str, Tuple[str, ...]] = {
    "doc_md_python_callable_file": ("doc_md_python_callable_name",),
    "doc_md_python_callable_name": ("doc_md_python_callable_file",),
    "doc_md_python_arguments": ("doc_md_python_callable_name",),
    "retry_exponential_backoff": ("max_retry_delay",),
}


def _check_metadata() -> None:
    """Fail at import if an entry has a typo'd field or an impossible bound."""
    for key, meta in PARAM_METADATA.items():
        unknown = set(meta) - _FIELDS
        if unknown:
            raise RuntimeError(f"'{key}' has unknown metadata field(s): {sorted(unknown)}")
        for bound in ("min_version", "max_version", "deprecated_since"):
            if meta.get(bound) is not None:
                Version(meta[bound])
        if meta.get("min_version") and meta.get("max_version"):
            if Version(meta["min_version"]) >= Version(meta["max_version"]):
                raise RuntimeError(f"'{key}' has an empty version range")
        deprecated_in_favor_of = meta.get("deprecated_in_favor_of")
        if deprecated_in_favor_of and deprecated_in_favor_of not in PARAM_METADATA:
            raise RuntimeError(f"'{key}' defers to unknown key '{deprecated_in_favor_of}'")
        if meta.get("transform") and not callable(meta["transform"]):
            raise RuntimeError(f"'{key}' has a non-callable transform")
        if meta.get("pattern"):
            re.compile(meta["pattern"])
    for key, needs in REQUIRES.items():
        for other in (key,) + tuple(needs):
            if other not in PARAM_METADATA:
                raise RuntimeError(f"REQUIRES names unknown key '{other}'")
    for fields, _ in MUTUALLY_EXCLUSIVE:
        for key in fields:
            if key not in PARAM_METADATA:
                raise RuntimeError(f"MUTUALLY_EXCLUSIVE names unknown key '{key}'")


_check_metadata()


@lru_cache(maxsize=None)
def keys_in_scope(scope: str) -> frozenset:
    """Every configuration key valid in *scope*."""
    return frozenset(k for k, m in PARAM_METADATA.items() if scope in m.get("scope", DAG))


@lru_cache(maxsize=1)
def dag_argument_names() -> frozenset:
    """DAG-level keys ``_build_dag_kwargs`` may forward to the DAG constructor."""
    return frozenset(
        key
        for key in keys_in_scope("dag")
        if not PARAM_METADATA[key].get("dagfactory_only")
        and not PARAM_METADATA[key].get("handled")
        and PARAM_METADATA[key].get("supported", True)
    )


def unsupported_reason(key: str, airflow_version: Version) -> Optional[Tuple[str, str]]:
    """Why the builder will not forward *key*, as ``(severity, message)``.

    Returns ``None`` when the key applies. A key outside its Airflow version
    range is an error: the author asked for something this Airflow cannot do.
    A key dag-factory simply does not wire through is a warning: the config is
    valid, the value is just ignored.
    """
    meta = PARAM_METADATA.get(key, {})

    min_version = meta.get("min_version")
    if min_version and airflow_version < Version(min_version):
        return (
            ERROR,
            f"`{key}` was introduced in Airflow {min_version}; the configured Airflow is {airflow_version}.",
        )

    max_version = meta.get("max_version")
    if max_version and airflow_version >= Version(max_version):
        return (
            ERROR,
            f"`{key}` was removed in Airflow {max_version}; the configured Airflow is {airflow_version}.",
        )

    if meta.get("supported", True) is False:
        return (
            WARNING,
            f"`{key}` is a valid Airflow DAG argument but is not wired through dag-factory; "
            f"the value is ignored (see {UNSUPPORTED_ARGS_ISSUE}).",
        )
    return None


# ---------------------------------------------------------------------------
# Checking
# ---------------------------------------------------------------------------
#: Values that arrive as real Python objects because ``cast_with_type`` has
#: already run over the YAML: a ``__type__`` directive materialises timetables,
#: datasets, timedeltas and callables. Anything that is not a JSON primitive is
#: therefore accepted wherever a mapping or a string was declared.
_JSON_PRIMITIVES = (str, int, float, bool, list, dict, type(None))


def _type_ok(value: Any, expected: Tuple[type, ...]) -> bool:
    if isinstance(value, bool) and bool not in expected:
        return False  # bool is an int subclass; don't let it satisfy `int`
    if isinstance(value, expected):
        return True
    if not isinstance(value, _JSON_PRIMITIVES):
        return True  # materialised by cast_with_type
    if str in expected:
        # Dates parse out of YAML as date/datetime objects.
        import datetime

        return isinstance(value, (datetime.date, datetime.datetime))
    return False


def _field_present(config: Dict[str, Any], key: str, scope: str) -> bool:
    if key in config and config[key] is not None:
        return True
    if scope == "dag" and "default_args" in scope_of(key):
        nested = config.get("default_args") or {}
        return isinstance(nested, dict) and nested.get(key) is not None
    return False


def scope_of(key: str) -> Tuple[str, ...]:
    return tuple(PARAM_METADATA.get(key, {}).get("scope", DAG))


def check(
    config: Dict[str, Any],
    airflow_version: Version,
    scope: str = "dag",
    check_values: bool = True,
    report_unknown: bool = True,
) -> List[Tuple[str, str, str]]:
    """Check one section of a resolved config against the metadata.

    *scope* says which section: the DAG body, a task, or ``default_args``.
    *check_values* covers types, enums, minimums and patterns; callers turn it
    off for sections whose values reach an Airflow operator, because Airflow
    validates operator arguments itself and says it better. *report_unknown*
    is turned off for a section whose keys are mostly not dag-factory
    parameters at all.

    Use :func:`check_for_build` rather than calling this directly; it encodes
    which sections the builder should check.

    The config must already be resolved — defaults merged, values cast.
    """
    findings: List[Tuple[str, str, str]] = []
    known = keys_in_scope(scope)

    for key, value in config.items():
        if key not in known:
            if key in PARAM_METADATA:
                findings.append((WARNING, key, f"`{key}` is not valid at {scope} level."))
            elif report_unknown:
                findings.append((WARNING, key, f"`{key}` is not a known dag-factory parameter."))
            continue

        meta = PARAM_METADATA[key]

        reason = unsupported_reason(key, airflow_version)
        if reason:
            severity, message = reason
            findings.append((severity, key, message))
            continue

        deprecated_since = meta.get("deprecated_since")
        if deprecated_since and airflow_version >= Version(deprecated_since):
            deprecated_in_favor_of = meta.get("deprecated_in_favor_of")
            hint = f" Use `{deprecated_in_favor_of}` instead." if deprecated_in_favor_of else ""
            findings.append((WARNING, key, f"`{key}` is deprecated as of Airflow {deprecated_since}.{hint}"))

        if value is None or not check_values:
            continue
        expected_types = meta.get("types")
        if expected_types and not _type_ok(value, expected_types):
            names = ", ".join(t.__name__ for t in expected_types)
            findings.append((ERROR, key, f"`{key}` should be {names}, got {type(value).__name__}."))
            continue
        if meta.get("enum") and value not in meta["enum"]:
            findings.append((ERROR, key, f"`{key}` should be one of {', '.join(map(str, meta['enum']))}."))
        if meta.get("minimum") is not None and isinstance(value, (int, float)) and value < meta["minimum"]:
            findings.append((ERROR, key, f"`{key}` should be at least {meta['minimum']}."))
        if meta.get("pattern") and isinstance(value, str) and not re.fullmatch(meta["pattern"], value):
            findings.append((ERROR, key, f"`{key}` should match {meta['pattern']}."))

    # Required-ness is a property of the whole config, not of one section: a
    # start_date under default_args satisfies the DAG. Only the top-level pass
    # decides it, so the nested default_args pass does not re-report it.
    for key in sorted(known) if scope == "dag" else ():
        meta = PARAM_METADATA[key]
        if meta.get("required") and not _field_present(config, key, scope):
            where = " or under default_args" if "default_args" in scope_of(key) else ""
            findings.append((ERROR, key, f"`{key}` must be set on the DAG body{where}."))

    for fields, message in MUTUALLY_EXCLUSIVE:
        present = [f for f in fields if f in known and config.get(f) is not None]
        if len(present) > 1:
            listed = ", ".join(f"`{f}`" for f in fields)
            findings.append((ERROR, present[0], message.format(fields=listed) + f" (set: {', '.join(present)})"))

    for key, needs in REQUIRES.items():
        if key in known and config.get(key) is not None:
            for other in needs:
                if config.get(other) is None:
                    findings.append((ERROR, key, f"`{key}` also requires `{other}` to be set."))

    return findings


def _under(prefix: str, findings: List[Tuple[str, str, str]]) -> List[Tuple[str, str, str]]:
    return [(severity, f"{prefix}.{path}", message) for severity, path, message in findings]


def check_for_build(config: Dict[str, Any], airflow_version: Version) -> List[Tuple[str, str, str]]:
    """The checks worth running while building a DAG.

    Only what Airflow accepts silently. A DAG argument of the wrong type, a
    parameter this Airflow no longer takes, a key at the wrong level, a
    misspelling — each of those builds a subtly wrong DAG with no complaint
    from anyone, so they are worth reporting.

    Everything Airflow reports for itself is left to Airflow. Values inside
    ``default_args`` reach an operator, which type-checks them and raises with
    a better message, so their shape is not checked here. Tasks are skipped
    entirely for the same reason: an unimportable operator raises ImportError,
    a bad argument raises TypeError, a missing dependency raises KeyError and
    a cycle raises ValueError.
    """
    findings = check(config, airflow_version)
    default_args = config.get("default_args")
    if isinstance(default_args, dict):
        findings += _under(
            "default_args",
            check(default_args, airflow_version, scope="default_args", check_values=False),
        )
    return findings
