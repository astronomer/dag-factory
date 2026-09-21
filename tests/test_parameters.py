"""Tests for the parameter metadata that drives DAG building."""

import datetime

import pytest
from packaging.version import Version

from dagfactory import parameters
from dagfactory.parameters import (
    ERROR,
    PARAM_METADATA,
    WARNING,
    check,
    check_for_build,
    check_for_lint,
    check_tasks,
    dag_argument_names,
    keys_in_scope,
)


class TestMetadataIntegrity:
    """The table is plain dicts, so these guards stand in for a type checker."""

    def test_no_entry_has_an_unknown_field(self):
        # _check_metadata() runs at import; this pins the failure mode.
        with pytest.raises(RuntimeError, match="unknown metadata field"):
            original = dict(PARAM_METADATA["catchup"])
            PARAM_METADATA["catchup"] = {**original, "min_verison": "2.9.0"}
            try:
                parameters._check_metadata()
            finally:
                PARAM_METADATA["catchup"] = original

    def test_alias_targets_exist(self):
        for key, meta in PARAM_METADATA.items():
            target = meta.get("deprecated_in_favor_of")
            if target:
                assert target in PARAM_METADATA, f"'{key}' defers to unknown '{target}'"

    def test_every_transform_is_callable_and_forwarded(self):
        forwarded = dag_argument_names()
        for key, meta in PARAM_METADATA.items():
            if meta.get("transform"):
                assert callable(meta["transform"])
                assert key in forwarded, f"'{key}' has a transform but is never forwarded"

    def test_every_entry_declares_a_scope_we_recognise(self):
        for key, meta in PARAM_METADATA.items():
            assert set(meta.get("scope", ("dag",))) <= {"dag", "task", "default_args"}, key

    def test_version_bounds_parse_and_are_ordered(self):
        for key, meta in PARAM_METADATA.items():
            if meta.get("min_version") and meta.get("max_version"):
                assert Version(meta["min_version"]) < Version(meta["max_version"]), key


class TestForwarding:
    def test_dagfactory_only_keys_are_not_forwarded(self):
        forwarded = dag_argument_names()
        for key in ("tasks", "task_groups", "doc_md_file_path", "timezone"):
            assert key not in forwarded

    def test_handled_keys_are_not_forwarded(self):
        forwarded = dag_argument_names()
        for key in ("schedule", "schedule_interval", "tags"):
            assert key not in forwarded

    def test_unsupported_keys_are_not_forwarded(self):
        forwarded = dag_argument_names()
        for key in ("fail_fast", "owner_links", "template_undefined"):
            assert key not in forwarded

    def test_ordinary_dag_arguments_are_forwarded(self):
        forwarded = dag_argument_names()
        for key in ("dag_id", "catchup", "max_active_runs", "default_args", "start_date"):
            assert key in forwarded

    def test_scopes_partition_as_expected(self):
        # params is the one key valid everywhere: DAG() and BaseOperator both take it.
        assert len(keys_in_scope("dag")) == 43
        assert len(keys_in_scope("task")) == 20
        assert len(keys_in_scope("default_args")) == 26
        assert keys_in_scope("dag") & keys_in_scope("task") == {"params"}


class TestVersionRules:
    def test_max_version_is_exclusive(self):
        assert parameters.unsupported_reason("timetable", Version("2.10.5")) is None
        severity, message = parameters.unsupported_reason("timetable", Version("3.0.0"))
        assert severity == ERROR and "removed in Airflow 3.0.0" in message

    def test_min_version_is_inclusive(self):
        assert parameters.unsupported_reason("deadline", Version("3.1.0"))[0] == WARNING  # unsupported, but in range
        severity, message = parameters.unsupported_reason("deadline", Version("3.0.0"))
        assert severity == ERROR and "introduced in Airflow 3.1.0" in message

    def test_unwired_parameter_is_only_a_warning(self):
        severity, message = parameters.unsupported_reason("fail_fast", Version("3.0.0"))
        assert severity == WARNING and "not wired through dag-factory" in message

    def test_version_range_beats_the_unwired_flag(self):
        """allowed_run_types is both unwired and 3.2+; the range decides severity."""
        severity, _ = parameters.unsupported_reason("allowed_run_types", Version("3.0.0"))
        assert severity == ERROR

    def test_deprecated_alias_has_no_version_ceiling(self):
        """concurrency is rewritten to max_active_tasks, so it survives Airflow 3."""
        assert parameters.unsupported_reason("concurrency", Version("3.3.0")) is None


class TestCheck:
    AF3 = Version("3.0.0")
    #: A task that actually builds, so task-level checks have something real.
    TASK = {"operator": "airflow.providers.standard.operators.bash.BashOperator", "bash_command": "echo"}

    def _keys(self, findings):
        return {(s, p) for s, p, _ in findings}

    def test_a_minimal_valid_config_is_clean(self):
        assert check({"dag_id": "d", "start_date": "2024-01-01", "tasks": {"t": self.TASK}}, self.AF3) == []

    def test_unknown_key_is_a_warning(self):
        assert (WARNING, "nonsense") in self._keys(
            check({"dag_id": "d", "start_date": "2024-01-01", "tasks": {"t": self.TASK}, "nonsense": 1}, self.AF3)
        )

    def test_wrong_type_is_an_error(self):
        findings = check(
            {"dag_id": "d", "start_date": "2024-01-01", "tasks": {"t": self.TASK}, "catchup": "yes"}, self.AF3
        )
        assert (ERROR, "catchup") in self._keys(findings)

    def test_bool_does_not_satisfy_int(self):
        findings = check(
            {"dag_id": "d", "start_date": "2024-01-01", "tasks": {"t": self.TASK}, "max_active_runs": True}, self.AF3
        )
        assert (ERROR, "max_active_runs") in self._keys(findings)

    def test_enum_violation_is_an_error(self):
        findings = check(
            {"dag_id": "d", "start_date": "2024-01-01", "tasks": {"t": self.TASK}, "orientation": "sideways"},
            Version("2.10.0"),
        )
        assert (ERROR, "orientation") in self._keys(findings)

    def test_minimum_is_enforced(self):
        findings = check(
            {"dag_id": "d", "start_date": "2024-01-01", "tasks": {"t": self.TASK}, "max_active_runs": 0}, self.AF3
        )
        assert (ERROR, "max_active_runs") in self._keys(findings)

    def test_materialised_python_objects_pass_type_checks(self):
        """cast_with_type turns __type__ directives into real objects."""
        config = {
            "dag_id": "d",
            "start_date": datetime.datetime(2024, 1, 1),
            "tasks": {"t": self.TASK},
            "timetable": object(),
        }
        assert (ERROR, "start_date") not in self._keys(check(config, Version("2.10.0")))
        assert (ERROR, "timetable") not in self._keys(check(config, Version("2.10.0")))

    def test_start_date_may_come_from_default_args(self):
        config = {"dag_id": "d", "tasks": {"t": self.TASK}, "default_args": {"start_date": "2024-01-01"}}
        assert check(config, self.AF3) == []

    def test_start_date_missing_everywhere_is_an_error(self):
        assert (ERROR, "start_date") in self._keys(check({"dag_id": "d", "tasks": {"t": self.TASK}}, self.AF3))

    def test_mutually_exclusive_keys_error(self):
        config = {
            "dag_id": "d",
            "start_date": "2024-01-01",
            "tasks": {"t": self.TASK},
            "doc_md": "x",
            "doc_md_file_path": "/a",
        }
        assert any("single source" in m for _, _, m in check(config, self.AF3))

    def test_dependent_key_is_required(self):
        config = {
            "dag_id": "d",
            "start_date": "2024-01-01",
            "tasks": {"t": self.TASK},
            "doc_md_python_callable_file": "/a.py",
        }
        assert any("also requires" in m for _, _, m in check(config, self.AF3))

    def test_default_args_are_checked_with_task_semantics(self):
        config = {
            "dag_id": "d",
            "tasks": {"t": self.TASK},
            "default_args": {"start_date": "2024-01-01", "retries": "lots"},
        }
        assert (ERROR, "default_args.retries") in self._keys(check_for_lint(config, self.AF3))

    def test_a_dag_only_key_inside_default_args_is_flagged(self):
        config = {
            "dag_id": "d",
            "tasks": {"t": self.TASK},
            "default_args": {"start_date": "2024-01-01", "catchup": False},
        }
        assert (WARNING, "default_args.catchup") in self._keys(check_for_lint(config, self.AF3))

    def test_deprecation_is_a_warning(self):
        config = {"dag_id": "d", "start_date": "2024-01-01", "tasks": {"t": self.TASK}, "concurrency": 4}
        assert (WARNING, "concurrency") in self._keys(check(config, self.AF3))


BASH = "airflow.providers.standard.operators.bash.BashOperator"


class TestTaskChecks:
    """Task parameters are checked with task-level semantics."""

    AF3 = Version("3.0.0")

    def _check(self, tasks, **extra):
        """What lint runs: the config's own parameters, then the tasks."""
        config = {"dag_id": "d", "start_date": "2024-01-01", "tasks": tasks, **extra}
        return check(config, self.AF3) + check_tasks(config, self.AF3)

    def _keys(self, findings):
        return {(s, p) for s, p, _ in findings}

    def test_a_valid_task_is_clean(self):
        assert self._check({"t": {"operator": BASH, "bash_command": "echo"}}) == []

    def test_wrong_type_on_a_task_parameter(self):
        findings = self._check({"t": {"operator": BASH, "bash_command": "echo", "retries": "many"}})
        assert (ERROR, "tasks.t.retries") in self._keys(findings)

    def test_task_parameter_removed_in_this_airflow(self):
        """sla goes in Airflow 3.1, so 3.0 only deprecates it."""
        task = {"t": {"operator": BASH, "bash_command": "echo", "sla": 300}}
        base = {"dag_id": "d", "start_date": "2024-01-01", "tasks": task}

        at_30 = check_tasks(base, Version("3.0.0"))
        assert (WARNING, "tasks.t.sla") in {(s, p) for s, p, _ in at_30}

        at_31 = check_tasks(base, Version("3.1.0"))
        assert (ERROR, "tasks.t.sla") in {(s, p) for s, p, _ in at_31}

    def test_operator_keyword_arguments_are_not_flagged(self):
        """bash_command is not in the table; the operator accepts it."""
        findings = self._check({"t": {"operator": BASH, "bash_command": "echo", "env": {"A": "1"}}})
        assert findings == []

    def test_dagfactory_task_keys_are_not_flagged(self):
        findings = self._check(
            {
                "a": {"operator": BASH, "bash_command": "echo"},
                "b": {"operator": BASH, "bash_command": "echo", "dependencies": ["a"], "task_id": "b"},
            }
        )
        assert findings == []

    def test_unrecognised_task_keys_are_left_alone(self):
        """Most operators take **kwargs, so a signature cannot spot a typo.

        Airflow decides at construction and raises `Invalid arguments were
        passed`, so guessing here would only risk false positives.
        """
        findings = self._check({"t": {"operator": BASH, "bash_command": "echo", "nonsense_key": 1}})
        assert findings == []

    def test_unimportable_operator_is_an_error(self):
        findings = self._check({"t": {"operator": "airflow.operators.bash.NoSuchOperator"}})
        assert any("Cannot import" in m for _, _, m in findings)

    def test_only_the_import_error_is_reported_for_a_broken_operator(self):
        findings = self._check({"t": {"operator": "no.such.module.Operator", "whatever": 1}})
        assert [p for _, p, _ in findings] == ["tasks.t"]

    def test_a_task_must_declare_operator_or_decorator(self):
        findings = self._check({"t": {"bash_command": "echo"}})
        assert any("operator" in m and "decorator" in m for _, _, m in findings)

    def test_a_decorator_task_is_accepted(self):
        findings = self._check({"t": {"decorator": "airflow.sdk.task", "python_callable_name": "f"}})
        assert not [f for f in findings if "operator" in f[2] and "decorator" in f[2]]

    def test_a_task_that_is_not_a_mapping_is_an_error(self):
        assert (ERROR, "tasks.not_a_map") in self._keys(self._check({"not_a_map": "oops"}))


class TestDependencyChecks:
    AF3 = Version("3.0.0")

    def _check(self, tasks, task_groups=None):
        config = {"dag_id": "d", "start_date": "2024-01-01", "tasks": tasks}
        if task_groups is not None:
            config["task_groups"] = task_groups
        return check(config, self.AF3) + check_tasks(config, self.AF3)

    def test_dependency_on_a_missing_task_is_an_error(self):
        findings = self._check({"t": {"operator": BASH, "bash_command": "e", "dependencies": ["nope"]}})
        assert any("does not exist" in m for _, _, m in findings)

    def test_dependency_on_a_real_task_is_fine(self):
        findings = self._check(
            {
                "a": {"operator": BASH, "bash_command": "e"},
                "b": {"operator": BASH, "bash_command": "e", "dependencies": ["a"]},
            }
        )
        assert findings == []

    def test_dependency_on_a_task_group_is_fine(self):
        findings = self._check(
            {"a": {"operator": BASH, "bash_command": "e", "dependencies": ["tg"]}},
            task_groups={"tg": {"tooltip": "x"}},
        )
        assert findings == []

    def test_a_cycle_is_an_error(self):
        findings = self._check(
            {
                "a": {"operator": BASH, "bash_command": "e", "dependencies": ["b"]},
                "b": {"operator": BASH, "bash_command": "e", "dependencies": ["a"]},
            }
        )
        assert any("Cycle detected" in m for _, _, m in findings)

    def test_a_long_chain_is_not_a_cycle(self):
        tasks = {"t0": {"operator": BASH, "bash_command": "e"}}
        for i in range(1, 6):
            tasks[f"t{i}"] = {"operator": BASH, "bash_command": "e", "dependencies": [f"t{i-1}"]}
        assert self._check(tasks) == []


class TestPattern:
    def test_dag_id_pattern_is_enforced(self):
        findings = check(
            {
                "dag_id": "has spaces!",
                "start_date": "2024-01-01",
                "tasks": {"t": {"operator": BASH, "bash_command": "e"}},
            },
            Version("3.0.0"),
        )
        assert any("should match" in m for _, _, m in findings)

    def test_a_valid_dag_id_passes(self):
        findings = check(
            {
                "dag_id": "fine.dag-id_1",
                "start_date": "2024-01-01",
                "tasks": {"t": {"operator": BASH, "bash_command": "e"}},
            },
            Version("3.0.0"),
        )
        assert findings == []


class TestBuildChecksOnlyWhatAirflowMisses:
    """check_for_build is what DagBuilder.build runs.

    Anything Airflow reports for itself is left to Airflow, so the same
    problem is never reported twice. Anything Airflow accepts silently has to
    be reported here or nowhere.
    """

    AF3 = Version("3.0.0")

    def _config(self, task=None, **extra):
        config = {
            "dag_id": "d",
            "start_date": "2024-01-01",
            "tasks": {"t": task or {"operator": BASH, "bash_command": "e"}},
        }
        config.update(extra)
        return config

    def _paths(self, findings):
        return {p for _, p, _ in findings}

    # --- left to Airflow -------------------------------------------------
    def test_skips_task_checks(self):
        for task in (
            {"operator": "no.such.module.Operator"},
            {"operator": BASH, "bash_command": "e", "retries": "many"},
            {"operator": BASH, "bash_command": "e", "dependencies": ["ghost"]},
        ):
            config = self._config(task)
            assert check_for_build(config, self.AF3) == [], task
            assert check_for_lint(config, self.AF3) != [], task

    def test_skips_the_shape_of_default_args_values(self):
        """These reach an operator, which type-checks them and raises."""
        config = self._config(default_args={"start_date": "2024-01-01", "retries": "many"})
        assert "default_args.retries" not in self._paths(check_for_build(config, self.AF3))
        assert "default_args.retries" in self._paths(check_for_lint(config, self.AF3))

    # --- silent in Airflow, so still checked ------------------------------
    def test_reports_dag_level_problems(self):
        config = self._config(catchup="notabool", nonsense_key=1)
        assert {"catchup", "nonsense_key"} <= self._paths(check_for_build(config, self.AF3))

    def test_reports_a_dag_key_misplaced_in_default_args(self):
        config = self._config(default_args={"start_date": "2024-01-01", "catchup": False})
        assert "default_args.catchup" in self._paths(check_for_build(config, self.AF3))

    def test_reports_an_unknown_key_in_default_args(self):
        config = self._config(default_args={"start_date": "2024-01-01", "nonsense": 1})
        assert "default_args.nonsense" in self._paths(check_for_build(config, self.AF3))

    def test_reports_a_version_gated_key_in_default_args(self):
        config = self._config(default_args={"start_date": "2024-01-01", "sla": 300})
        assert "default_args.sla" in self._paths(check_for_build(config, Version("3.1.0")))
