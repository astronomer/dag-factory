import pytest
from dagfactory.dag_codegen import (
    build_task_group_tree,
    generate_task_code,
    get_operator_import,
    generate_dag_file,
    generate_dag_block,
    resolve_taskflow_value,
    resolve_expand_value,
    order_tasks_by_reference,
    get_callable_arg_names,
    format_value,
    generate_doc_md_statements,
    resolve_callable_pairs,
    resolve_variables_as_arguments,
)

def test_get_operator_import():
    result = get_operator_import("airflow.operators.bash.BashOperator")
    assert result == "from airflow.operators.bash import BashOperator"

def test_get_operator_import_invalid():
    with pytest.raises(ValueError):
        get_operator_import("BashOperator")

def test_generate_task_code():                                                                                                                                                                                                                     
    task_config = {                                                                                                                                                                                                                                
        "operator": "airflow.operators.bash.BashOperator",                                                                                                                                                                                         
        "bash_command": "echo hello",                                                                                                                                                                                                              
    }                                                                                                                                                                                                                                              
    result = generate_task_code("task_a", task_config)                                                                                                                                                                                             
    assert "task_a = BashOperator(" in result
    assert "task_id='task_a'" in result                                                                                                                                                                                                            
    assert "bash_command='echo hello'" in result


def test_generate_dag_file():                                                                                                                                                                                                                      
    dag_config = {                                                                                                                                                                                                                                 
          "schedule": "@daily",                                                                                                                                                                                                                      
          "start_date": "2024-01-01",                                                                                                                                                                                                                
          "tasks": {                                        
              "task_a": {                                                                                                                                                                                                                            
                  "operator": "airflow.operators.bash.BashOperator",
                  "bash_command": "echo hello",
              }                                                                                                                                                                                                                                      
          }
    }                                                                                                                                                                                                                                              
    result = generate_dag_file("my_dag", dag_config)
    assert "from airflow.operators.bash import BashOperator" in result
    assert "dag_id='my_dag'" in result
    assert "task_a = BashOperator(" in result


def test_generate_dag_block_generates_dependencies():
    dag_config = {
        "tasks": {
            "task_a": {"operator": "airflow.operators.bash.BashOperator", "bash_command": "echo a"},
            "task_b": {
                "operator": "airflow.operators.bash.BashOperator",
                "bash_command": "echo b",
                "dependencies": ["task_a"],
            },
        }
    }
    block, _ = generate_dag_block("my_dag", dag_config)
    assert "task_a >> task_b" in block
    # 'dependencies' must not leak into the operator's constructor kwargs
    assert "dependencies=" not in block


def test_generate_dag_block_passes_default_args_to_dag_constructor():
    dag_config = {
        "default_args": {"retries": 2},
        "tasks": {
            "task_a": {"operator": "airflow.operators.bash.BashOperator", "bash_command": "echo a"},
        },
    }
    block, _ = generate_dag_block("my_dag", dag_config)
    assert "default_args={'retries': 2}" in block
    # Airflow itself propagates default_args to tasks; codegen must not duplicate them per task.
    assert "retries=2" not in block.split("with dag_my_dag:")[1]


def test_generate_dag_block_default_args_with_start_date_uses_pendulum():
    dag_config = {
        "default_args": {"start_date": "2024-01-01", "owner": "global_owner"},
        "tasks": {
            "task_a": {"operator": "airflow.operators.bash.BashOperator", "bash_command": "echo a"},
        },
    }
    block, imports = generate_dag_block("my_dag", dag_config)
    assert "default_args={'start_date': pendulum.parse(\"2024-01-01T00:00:00+00:00\"), 'owner': 'global_owner'}" in block
    assert "import pendulum" in imports


def test_generate_dag_block_task_level_overrides_default_args():
    dag_config = {
        "default_args": {"retries": 2},
        "tasks": {
            "task_a": {
                "operator": "airflow.operators.bash.BashOperator",
                "bash_command": "echo a",
                "retries": 5,
            },
        },
    }
    block, _ = generate_dag_block("my_dag", dag_config)
    task_block = block.split("with dag_my_dag:")[1]
    assert "retries=5" in task_block


def test_generate_dag_block_renders_task_group():
    dag_config = {
        "task_groups": {"group_1": {"tooltip": "a group"}},
        "tasks": {
            "task_a": {
                "operator": "airflow.operators.bash.BashOperator",
                "bash_command": "echo a",
                "task_group_name": "group_1",
            },
        },
    }
    block, imports = generate_dag_block("my_dag", dag_config)
    assert "with TaskGroup(group_id='group_1', tooltip='a group') as tg_group_1:" in block
    assert "task_a = BashOperator(" in block
    assert "from airflow.sdk import TaskGroup" in imports
    # a task inside a group must not also appear as a top-level task under `with dag_my_dag:`
    assert block.count("task_a = BashOperator(") == 1


def test_generate_dag_block_uses_dag_name_as_dag_id_without_prefix():
    dag_config = {
        "tasks": {
            "task_a": {"operator": "airflow.operators.bash.BashOperator", "bash_command": "echo a"},
        },
    }
    block, _ = generate_dag_block("my_dag", dag_config)
    assert "dag_id='my_dag'" in block


def test_generate_dag_block_parses_start_date_as_datetime():
    dag_config = {
        "start_date": "2024-01-01",
        "tasks": {
            "task_a": {"operator": "airflow.operators.bash.BashOperator", "bash_command": "echo a"},
        },
    }
    block, imports = generate_dag_block("my_dag", dag_config)
    assert 'start_date=pendulum.parse("2024-01-01T00:00:00+00:00")' in block
    assert "import pendulum" in imports


# --- build_task_group_tree ---


def test_build_task_group_tree_returns_nodes_and_roots():
    task_groups = {"grupo_a": {"tooltip": "a"}}
    nodes, roots = build_task_group_tree(task_groups)
    assert roots == ["grupo_a"]
    assert nodes["grupo_a"]["parent"] is None
    assert nodes["grupo_a"]["children_groups"] == []
    assert nodes["grupo_a"]["conf"] == {"tooltip": "a"}


def test_build_task_group_tree_flat_groups_are_all_roots():
    task_groups = {"grupo_a": {}, "grupo_b": {}}
    nodes, roots = build_task_group_tree(task_groups)
    assert sorted(roots) == ["grupo_a", "grupo_b"]
    assert nodes["grupo_a"]["children_groups"] == []
    assert nodes["grupo_b"]["children_groups"] == []


def test_build_task_group_tree_links_parent_and_child():
    task_groups = {
        "grupo_padre": {"tooltip": "grupo raiz"},
        "grupo_hijo": {"tooltip": "grupo anidado", "parent_group_name": "grupo_padre"},
    }
    nodes, roots = build_task_group_tree(task_groups)
    assert roots == ["grupo_padre"]
    assert nodes["grupo_padre"]["children_groups"] == ["grupo_hijo"]
    assert nodes["grupo_hijo"]["parent"] == "grupo_padre"


def test_build_task_group_tree_three_level_nesting():
    task_groups = {
        "abuelo": {},
        "padre": {"parent_group_name": "abuelo"},
        "hijo": {"parent_group_name": "padre"},
    }
    nodes, roots = build_task_group_tree(task_groups)
    assert roots == ["abuelo"]
    assert nodes["abuelo"]["children_groups"] == ["padre"]
    assert nodes["padre"]["children_groups"] == ["hijo"]
    assert nodes["hijo"]["children_groups"] == []


def test_build_task_group_tree_unknown_parent_raises():
    task_groups = {"grupo_hijo": {"parent_group_name": "no_existe"}}
    with pytest.raises(ValueError, match="no_existe"):
        build_task_group_tree(task_groups)


def test_build_task_group_tree_self_parent_raises():
    task_groups = {"grupo_a": {"parent_group_name": "grupo_a"}}
    with pytest.raises(ValueError, match="[Cc]ircular"):
        build_task_group_tree(task_groups)


def test_build_task_group_tree_circular_dependency_raises():
    task_groups = {
        "grupo_a": {"parent_group_name": "grupo_b"},
        "grupo_b": {"parent_group_name": "grupo_a"},
    }
    with pytest.raises(ValueError, match="[Cc]ircular"):
        build_task_group_tree(task_groups)


# --- resolve_taskflow_value ---


def test_resolve_taskflow_value_plain_string_is_quoted():
    result = resolve_taskflow_value("number", "hello", set())
    assert result == '"hello"' or result == "'hello'"


def test_resolve_taskflow_value_plus_prefix_returns_bare_variable():
    result = resolve_taskflow_value("number", "+some_number", set())
    assert result == "some_number"
    # must NOT be quoted as a string literal — it's a reference to a Python variable
    assert '"' not in result
    assert "'" not in result


def test_resolve_taskflow_value_plus_prefix_sanitizes_identifier():
    result = resolve_taskflow_value("number", "+weird-task:id", set())
    assert result == "weird_task_id"


def test_resolve_taskflow_value_non_string_passthrough():
    result = resolve_taskflow_value("number", 2, set())
    assert result == "2"


def test_resolve_taskflow_value_datetime_key_still_uses_pendulum():
    imports = set()
    result = resolve_taskflow_value("start_date", "2024-01-01", imports)
    assert result == 'pendulum.parse("2024-01-01T00:00:00+00:00")'
    assert "import pendulum" in imports


# --- generate_task_code with 'decorator' (TaskFlow) ---


def test_generate_task_code_taskflow_simple_callable():
    task_config = {
        "decorator": "airflow.sdk.definitions.decorators.task",
        "python_callable": "sample.some_number",
    }
    imports = set()
    result = generate_task_code("some_number", task_config, imports)
    assert result == "some_number = task(task_id='some_number', python_callable=some_number)()"
    assert "from airflow.sdk.definitions.decorators import task" in imports
    assert "from sample import some_number" in imports


def test_generate_task_code_taskflow_with_kwarg():
    task_config = {
        "decorator": "airflow.sdk.definitions.decorators.task",
        "python_callable": "sample.double",
        "number": 2,
    }
    result = generate_task_code("double_number_from_arg", task_config, set())
    assert result == "double_number_from_arg = task(task_id='double_number_from_arg', python_callable=double)(number=2)"


def test_generate_task_code_taskflow_with_plus_prefixed_upstream_ref():
    task_config = {
        "decorator": "airflow.sdk.definitions.decorators.task",
        "python_callable": "sample.double",
        "number": "+some_number",
    }
    result = generate_task_code("double_number_from_task", task_config, set())
    assert result == "double_number_from_task = task(task_id='double_number_from_task', python_callable=double)(number=some_number)"
    # must be a bare reference, not a quoted string
    assert 'number="+some_number"' not in result
    assert "number='+some_number'" not in result


def test_generate_task_code_taskflow_python_callable_name_and_file():
    task_config = {
        "decorator": "airflow.sdk.definitions.decorators.task",
        "python_callable_name": "build_numbers_list",
        "python_callable_file": "/dags/sample.py",
    }
    imports = set()
    result = generate_task_code("numbers_list", task_config, imports)
    assert "get_python_callable('build_numbers_list', '/dags/sample.py')" in result
    assert "from dagfactory.utils import get_python_callable" in imports


# --- resolve_expand_value ---


def test_resolve_expand_value_dot_output_reference():
    result = resolve_expand_value("op_args", "request.output", set())
    assert result == "request.output"
    assert '"' not in result
    assert "'" not in result


def test_resolve_expand_value_dot_output_reference_sanitizes_identifier():
    result = resolve_expand_value("op_args", "weird-task:id.output", set())
    assert result == "weird_task_id.output"


def test_resolve_expand_value_xcomarg_reference():
    result = resolve_expand_value("op_args", "XcomArg(request)", set())
    assert result == "request.output"


def test_resolve_expand_value_plain_value_passthrough():
    imports = set()
    result = resolve_expand_value("number", [1, 3, 5], imports)
    assert result == "[1, 3, 5]"


# --- generate_task_code with 'expand'/'partial' (dynamic task mapping, regular operator) ---


def test_generate_task_code_expand_only():
    task_config = {
        "operator": "airflow.operators.python.PythonOperator",
        "expand": {"op_args": "request.output"},
    }
    result = generate_task_code("process", task_config, set())
    assert result == "process = PythonOperator.partial(\n    task_id='process',\n).expand(\n    op_args=request.output,\n)"


def test_generate_task_code_expand_with_partial():
    task_config = {
        "operator": "airflow.operators.python.PythonOperator",
        "partial": {"op_kwargs": {"fixed_param": "test"}},
        "expand": {"op_args": "request.output"},
    }
    result = generate_task_code("process", task_config, set())
    assert "PythonOperator.partial(" in result
    assert "task_id='process'" in result
    assert "op_kwargs={'fixed_param': 'test'}" in result
    assert ".expand(" in result
    assert "op_args=request.output" in result
    # 'partial' and 'expand' must not leak as literal constructor kwargs
    assert "partial={" not in result
    assert "expand={" not in result


# --- generate_task_code with 'expand'/'partial' (dynamic task mapping, TaskFlow) ---


def test_generate_task_code_taskflow_expand_only():
    task_config = {
        "decorator": "airflow.sdk.definitions.decorators.task",
        "python_callable": "sample.double",
        "expand": {"number": "+numbers_list"},
    }
    result = generate_task_code("double_mapped", task_config, set())
    expected = (
        "double_mapped = task(task_id='double_mapped', python_callable=double)"
        ".expand(number=numbers_list)"
    )
    assert result == expected


def test_generate_task_code_taskflow_expand_with_partial():
    task_config = {
        "decorator": "airflow.sdk.definitions.decorators.task",
        "python_callable": "sample.double_with_label",
        "expand": {"number": "+numbers_list"},
        "partial": {"label": True},
    }
    result = generate_task_code("double_mapped", task_config, set())
    expected = (
        "double_mapped = task(task_id='double_mapped', python_callable=double_with_label)"
        ".partial(label=True).expand(number=numbers_list)"
    )
    assert result == expected


def test_generate_task_code_taskflow_expand_rejects_leftover_callable_kwargs():
    task_config = {
        "decorator": "airflow.sdk.definitions.decorators.task",
        "python_callable": "sample.double",
        "expand": {"number": "+numbers_list"},
        "unrelated_kwarg": "boom",
    }
    with pytest.raises(ValueError, match="expand and partial"):
        generate_task_code("double_mapped", task_config, set())


# --- order_tasks_by_reference ---


# --- get_callable_arg_names ---


def test_get_callable_arg_names_resolves_real_dotted_string():
    # dagfactory.utils.check_dict_key(item_dict, key) — a real, stable, importable function.
    result = get_callable_arg_names({"python_callable": "dagfactory.utils.check_dict_key"})
    assert result == {"item_dict", "key"}


def test_get_callable_arg_names_returns_none_when_unresolvable():
    result = get_callable_arg_names({"python_callable": "no_such_module.no_such_func"})
    assert result is None


def test_get_callable_arg_names_returns_none_when_raw_code():
    from dagfactory.dag_codegen import _RawCode

    result = get_callable_arg_names({"python_callable": _RawCode("get_python_callable('a', 'b')")})
    assert result is None


def test_generate_task_code_taskflow_routes_non_callable_kwarg_to_decorator():
    # 'key' is a real arg of check_dict_key, but 'retries' is not — with real introspection,
    # 'retries' must go to the decorator call, not the wrapped function call.
    task_config = {
        "decorator": "airflow.sdk.definitions.decorators.task",
        "python_callable": "dagfactory.utils.check_dict_key",
        "item_dict": {"a": 1},
        "key": "a",
        "retries": 2,
    }
    result = generate_task_code("check_task", task_config, set())
    decorator_part, call_part = result.split(")(", 1)
    assert "retries=2" in decorator_part
    assert "item_dict=" in call_part
    assert "key=" in call_part
    assert "retries=" not in call_part


def test_order_tasks_by_reference_no_refs_keeps_original_order():
    tasks = {"a": {"operator": "x.Y"}, "b": {"operator": "x.Y"}}
    assert order_tasks_by_reference(tasks) == ["a", "b"]


def test_order_tasks_by_reference_reorders_when_referenced_out_of_order():
    # 'process' is defined BEFORE 'request' but references request.output inline —
    # request must come first in the generated source.
    tasks = {
        "process": {"operator": "x.Y", "expand": {"op_args": "request.output"}},
        "request": {"operator": "x.Y"},
    }
    order = order_tasks_by_reference(tasks)
    assert order.index("request") < order.index("process")


def test_order_tasks_by_reference_taskflow_plus_prefix():
    tasks = {
        "double_number_from_task": {"decorator": "x.task", "python_callable": "s.d", "number": "+some_number"},
        "some_number": {"decorator": "x.task", "python_callable": "s.n"},
    }
    order = order_tasks_by_reference(tasks)
    assert order.index("some_number") < order.index("double_number_from_task")


def test_generate_task_code_taskflow_expand_with_map_index_template():
    # map_index_template is a `@task` decorator-level kwarg, not a callable arg —
    # it must not be treated as a leftover callable kwarg that triggers a rejection.
    task_config = {
        "decorator": "airflow.decorators.task",
        "python_callable": "sample.extract_last_name",
        "map_index_template": "{{ custom_mapping_key }}",
        "expand": {"full_name": ["Lucy Black", "Vera Santos"]},
    }
    result = generate_task_code("dynamic_task", task_config, set())
    assert "map_index_template='{{ custom_mapping_key }}'" in result.split(".expand(")[0]
    assert ".expand(full_name=" in result


def test_order_tasks_by_reference_ignores_dependencies_key():
    # 'dependencies' is resolved via trailing '>>' statements, so it must NOT force reordering
    # (referencing a task only via 'dependencies' is fine regardless of definition order).
    tasks = {
        "b": {"operator": "x.Y", "dependencies": ["a"]},
        "a": {"operator": "x.Y"},
    }
    assert order_tasks_by_reference(tasks) == ["b", "a"]


# --- callback rendered as a dict ({callback: '...', **params}) ---


def test_format_value_callback_dict_is_callable_not_a_dict_literal():
    value = {
        "callback": "dagfactory.utils.check_dict_key",
        "item_dict": {"a": 1},
        "key": "a",
    }
    imports = set()
    result = format_value("on_failure_callback", value, imports)
    ns = {}
    exec("\n".join(sorted(imports)) + f"\nresult = {result}", ns)
    assert callable(ns["result"])


def test_format_value_callback_dict_wraps_plain_callable_in_partial():
    value = {"callback": "dagfactory.utils.check_dict_key", "item_dict": {"a": 1}, "key": "a"}
    imports = set()
    result = format_value("on_failure_callback", value, imports)
    assert result.startswith("partial(check_dict_key, ")
    assert "from functools import partial" in imports
    assert "from dagfactory.utils import check_dict_key" in imports


def test_format_value_callback_dict_instantiates_notifier_directly():
    value = {"callback": "airflow.sdk.bases.notifier.BaseNotifier"}
    imports = set()
    result = format_value("on_success_callback", value, imports)
    assert result == "BaseNotifier()"
    assert "from functools import partial" not in imports


# --- generate_doc_md_statements ---


def test_generate_doc_md_statements_none_when_absent():
    assert generate_doc_md_statements("my_dag", "dag_my_dag", {}, set()) == []


def test_generate_doc_md_statements_file_path():
    dag_config = {"doc_md_file_path": "/abs/path/doc.md"}
    result = generate_doc_md_statements("my_dag", "dag_my_dag", dag_config, set())
    assert len(result) == 1
    assert result[0] == "dag_my_dag.doc_md = open('/abs/path/doc.md', encoding=\"utf-8\").read()"


def test_generate_doc_md_statements_python_callable():
    dag_config = {
        "doc_md_python_callable_name": "build_docs",
        "doc_md_python_callable_file": "/abs/path/docs.py",
        "doc_md_python_arguments": {"version": "1.0"},
    }
    imports = set()
    result = generate_doc_md_statements("my_dag", "dag_my_dag", dag_config, imports)
    assert len(result) == 1
    assert "get_python_callable('build_docs', '/abs/path/docs.py')" in result[0]
    assert "dag_my_dag.doc_md = " in result[0]
    assert "from dagfactory.utils import get_python_callable" in imports


def test_generate_dag_block_places_doc_md_after_construction_not_as_kwarg():
    dag_config = {
        "doc_md_file_path": "/abs/path/doc.md",
        "tasks": {"task_a": {"operator": "airflow.operators.bash.BashOperator", "bash_command": "echo a"}},
    }
    block, _ = generate_dag_block("my_dag", dag_config)
    assert "doc_md_file_path=" not in block
    assert "dag_my_dag.doc_md = open('/abs/path/doc.md', encoding=\"utf-8\").read()" in block


# --- resolve_callable_pairs (HTTP sensor response_check, SqlSensor success/failure) ---


def test_resolve_callable_pairs_name_and_file():
    task_config = {"response_check_name": "check_ok", "response_check_file": "/abs/checks.py"}
    imports = set()
    resolve_callable_pairs(task_config, imports, {"response_check": "response_check"})
    assert "response_check_name" not in task_config
    assert "response_check_file" not in task_config
    assert task_config["response_check"] == "get_python_callable('check_ok', '/abs/checks.py')"
    assert "from dagfactory.utils import get_python_callable" in imports


def test_resolve_callable_pairs_lambda():
    task_config = {"success_check_lambda": "lambda x: x > 0"}
    imports = set()
    resolve_callable_pairs(task_config, imports, {"success": "success_check"})
    assert "success_check_lambda" not in task_config
    assert task_config["success"] == "get_python_callable_lambda('lambda x: x > 0')"
    assert "from dagfactory.utils import get_python_callable_lambda" in imports


def test_resolve_callable_pairs_different_output_key_and_prefix():
    # SqlSensor: input prefix 'failure_check', output key 'failure' — they differ.
    task_config = {"failure_check_name": "check_fail", "failure_check_file": "/abs/checks.py"}
    imports = set()
    resolve_callable_pairs(task_config, imports, {"failure": "failure_check"})
    assert "failure" in task_config
    assert "failure_check" not in task_config


def test_resolve_callable_pairs_noop_when_absent():
    task_config = {"bash_command": "echo 1"}
    resolve_callable_pairs(task_config, set(), {"response_check": "response_check"})
    assert task_config == {"bash_command": "echo 1"}


# --- resolve_variables_as_arguments ---


def test_resolve_variables_as_arguments_generates_variable_get_call():
    task_config = {
        "variables_as_arguments": [{"variable": "my_var", "attribute": "bash_command"}],
    }
    imports = set()
    resolve_variables_as_arguments(task_config, imports)
    assert "variables_as_arguments" not in task_config
    assert task_config["bash_command"] == "Variable.get('my_var', default=None)"
    assert any("import Variable" in i for i in imports)


def test_resolve_variables_as_arguments_multiple_variables():
    task_config = {
        "variables_as_arguments": [
            {"variable": "var_a", "attribute": "attr_a"},
            {"variable": "var_b", "attribute": "attr_b"},
        ],
    }
    resolve_variables_as_arguments(task_config, set())
    assert task_config["attr_a"] == "Variable.get('var_a', default=None)"
    assert task_config["attr_b"] == "Variable.get('var_b', default=None)"


def test_resolve_variables_as_arguments_noop_when_absent():
    task_config = {"bash_command": "echo 1"}
    resolve_variables_as_arguments(task_config, set())
    assert task_config == {"bash_command": "echo 1"}
