from pathlib import Path

import pytest
import yaml

from dagfactory.dagbuilder import BaseOperator, DagBuilder, DagFactoryConfigException
from tests.utils import check_dag_success, get_python_operator_path, run_dag

try:
    from airflow.sdk import XComArg

    TASK_DECORATOR_PATH = "airflow.sdk.task"
except ImportError:
    from airflow.models.xcom_arg import XComArg

    TASK_DECORATOR_PATH = "airflow.decorators.task"


class ValueOperator(BaseOperator):
    def __init__(self, value, fixed_param="test", **kwargs):
        super().__init__(**kwargs)
        self.value = value
        self.fixed_param = fixed_param

    def execute(self, context):
        return self.value


def publish_named_outputs(ti):
    result = {"a": [1, 2, 3, 4], "b": [10, 20]}
    for key, value in result.items():
        ti.xcom_push(key=key, value=value)
    return result


def forward(value):
    return value


@pytest.fixture
def mapping_config():
    path = Path(__file__).parent / "fixtures" / "named_xcom_mapping.yml"
    config = yaml.safe_load(path.read_text())["named_xcom_mapping"]
    for task in config["tasks"]:
        if task["operator"] == "airflow.operators.python.PythonOperator":
            task["operator"] = get_python_operator_path()
    return config


@pytest.mark.parametrize(
    "reference, key",
    [
        ('request.output["a"]', "a"),
        ("request.output['a']", "a"),
        ('request.output["a-b"]', "a-b"),
        ('request.output["a.b"]', "a.b"),
        ('request.output["a b"]', "a b"),
        ('request.output["a\\"b"]', 'a"b'),
        ('request.output[""]', ""),
        ("request.output", "return_value"),
        ("XcomArg(request)", "return_value"),
    ],
)
def test_expand_reference_preserves_native_xcom_key(reference, key):
    producer = ValueOperator(task_id="request", value=0)
    config = {"expand": {"value": reference}}

    resolved = DagBuilder.replace_expand_values(config, {"request": producer})

    ref = resolved["expand"]["value"]
    assert isinstance(ref, XComArg)
    assert ref.operator is producer
    assert ref.key == key


@pytest.mark.parametrize("reference", ['request.output["a"]', "request.output['a']"])
def test_builder_supports_named_xcom_forwarding_mapping(mapping_config, reference):
    mapping_config["tasks"][1]["op_kwargs"]["value"] = reference
    dag = DagBuilder("named_xcom_mapping", mapping_config, {}).build()["dag"]

    producer = dag.get_task("request")
    selected = dag.get_task("select_a")
    process = dag.get_task("process")
    ref = selected.op_kwargs["value"]
    assert isinstance(ref, XComArg)
    assert ref.operator is producer
    assert ref.key == "a"
    assert selected.upstream_task_ids == {"request"}
    expanded = process.expand_input.value["value"]
    assert expanded.operator is selected
    assert expanded.key == "return_value"
    assert process.partial_kwargs["fixed_param"] == "test"


@pytest.mark.parametrize(
    "reference, key",
    [('request.output["a"]', "a"), ("request.output['a']", "a"), ("request.output", "return_value")],
)
def test_classic_consumer_accepts_taskflow_output(mapping_config, reference, key):
    producer_config = mapping_config["tasks"][2]
    producer_config.pop("operator")
    producer_config["decorator"] = TASK_DECORATOR_PATH
    producer_config["multiple_outputs"] = True
    mapping_config["tasks"][1]["op_kwargs"]["value"] = reference

    dag = DagBuilder("taskflow_named_xcom", mapping_config, {}).build()["dag"]

    selected = dag.get_task("select_a")
    ref = selected.op_kwargs["value"]
    assert isinstance(ref, XComArg)
    assert ref.operator is dag.get_task("request")
    assert ref.key == key
    assert selected.upstream_task_ids == {"request"}


def test_builder_resolves_nested_templated_arguments(mapping_config):
    mapping_config["tasks"][1]["op_kwargs"]["value"] = {
        "refs": ['request.output["a"]', ("request.output['b']",)],
        "literal": "Text containing request.output is not a reference",
        "number": 12,
    }
    dag = DagBuilder("nested_named_xcom", mapping_config, {}).build()["dag"]

    value = dag.get_task("select_a").op_kwargs["value"]
    assert value["refs"][0].key == "a"
    assert isinstance(value["refs"][1], tuple)
    assert value["refs"][1][0].key == "b"
    assert value["literal"] == "Text containing request.output is not a reference"
    assert value["number"] == 12


def test_builder_resolves_templated_partial_arguments(mapping_config):
    selected_config = mapping_config["tasks"][1]
    selected_config["partial"] = {"op_kwargs": selected_config.pop("op_kwargs")}
    selected_config["expand"] = {"op_args": [[]]}
    dag = DagBuilder("partial_named_xcom", mapping_config, {}).build()["dag"]

    ref = dag.get_task("select_a").partial_kwargs["op_kwargs"]["value"]
    assert ref.operator is dag.get_task("request")
    assert ref.key == "a"


@pytest.mark.parametrize(
    "reference",
    [
        "request.output[0]",
        "request.output[None]",
        "request.output[a]",
        "request.output['a']['b']",
        "request.output[__import__('os').getcwd()]",
    ],
)
def test_expand_reference_rejects_nonliteral_string_keys(reference):
    producer = ValueOperator(task_id="request", value=0)
    with pytest.raises(DagFactoryConfigException, match="XCom key"):
        DagBuilder.replace_expand_values({"expand": {"value": reference}}, {"request": producer})


def test_output_reference_accepts_grouped_and_hyphenated_task_ids():
    producer = ValueOperator(task_id="producer-task", value=0)
    resolved = DagBuilder.replace_expand_values(
        {"expand": {"value": 'group.producer-task.output["a"]'}},
        {"group.producer-task": producer},
    )
    assert resolved["expand"]["value"].operator is producer
    assert resolved["expand"]["value"].key == "a"


def test_builder_retains_airflow_rejection_of_direct_named_key_mapping(mapping_config):
    mapping_config["tasks"].pop(1)
    process = mapping_config["tasks"][0]
    process["expand"]["value"] = 'request.output["a"]'
    process["dependencies"] = ["request"]

    with pytest.raises(ValueError, match="cannot map over XCom with custom key"):
        DagBuilder("direct_named_xcom", mapping_config, {}).build()


@pytest.mark.parametrize("values", [[1, 2, 3, 4], [{"value": 1}], ["plain", "a.output literal"]])
def test_static_mapping_values_remain_literal(values):
    resolved = DagBuilder.replace_expand_values({"expand": {"value": values}}, {})
    assert resolved["expand"]["value"] == values


def test_static_mapping_payloads_do_not_resolve_reference_shaped_strings():
    producer = ValueOperator(task_id="request", value=0)
    values = ['request.output["a"]', "missing.output", {"value": "request.output"}]
    resolved = DagBuilder.replace_expand_values({"expand": {"value": values}}, {"request": producer})
    assert resolved["expand"]["value"] is values


def test_missing_output_reference_explains_required_dependency(mapping_config):
    mapping_config["tasks"][1]["op_kwargs"]["value"] = 'missing.output["a"]'
    with pytest.raises(DagFactoryConfigException, match="Declare it in dependencies"):
        DagBuilder("missing_named_xcom", mapping_config, {}).build()


def test_existing_xcomarg_is_preserved():
    producer = ValueOperator(task_id="request", value=0)
    reference = producer.output
    resolved = DagBuilder.replace_expand_values({"expand": {"value": reference}}, {"request": producer})
    assert resolved["expand"]["value"] is reference


def test_builder_resolves_task_group_output_reference(mapping_config):
    mapping_config["task_groups"] = [{"group_name": "group"}]
    mapping_config["tasks"][2]["task_group_name"] = "group"
    mapping_config["tasks"][1]["op_kwargs"]["value"] = 'group.request.output["a"]'
    dag = DagBuilder("group_named_xcom", mapping_config, {}).build()["dag"]
    ref = dag.get_task("select_a").op_kwargs["value"]
    assert ref.operator is dag.get_task("group.request")
    assert ref.key == "a"


def test_non_templated_arguments_remain_literal(mapping_config):
    literal = 'request.output["a"]'
    mapping_config["tasks"][0]["partial"]["fixed_param"] = literal
    dag = DagBuilder("literal_named_xcom", mapping_config, {}).build()["dag"]
    assert dag.get_task("process").partial_kwargs["fixed_param"] == literal


@pytest.mark.integration
@pytest.mark.parametrize("taskflow_producer", [False, True])
def test_named_xcom_forwarding_maps_flat_values_at_runtime(mapping_config, taskflow_producer):
    if taskflow_producer:
        producer_config = mapping_config["tasks"][2]
        producer_config.pop("operator")
        producer_config["decorator"] = TASK_DECORATOR_PATH
        producer_config["multiple_outputs"] = True
    dag = DagBuilder("runtime_named_xcom", mapping_config, {}).build()["dag"]
    dag_run = run_dag(dag)
    assert check_dag_success(dag_run)
    instances = [ti for ti in dag_run.get_task_instances() if ti.task_id == "process"]
    assert sorted(ti.map_index for ti in instances) == [0, 1, 2, 3]
    values = [ti.xcom_pull(task_ids="process", key="return_value", map_indexes=ti.map_index) for ti in instances]
    assert sorted(values) == [1, 2, 3, 4]
