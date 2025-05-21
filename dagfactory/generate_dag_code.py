import argparse
import sys
import yaml
from dagfactory import DagFactory
import libcst as cst

def main():
    parser = argparse.ArgumentParser(description="Generate Airflow DAG code from a YAML configuration file.")
    parser.add_argument("config_file", help="Path to the YAML configuration file.")
    args = parser.parse_args()

    try:
        # Load YAML file and build DAG
        dag_factory = DagFactory(args.config_file)
        dags = dag_factory.build_dags()
        
        if not dags:
            print(f"No DAGs found in {args.config_file}", file=sys.stderr)
            sys.exit(1)

        # For simplicity, this script will generate code for the first DAG found.
        # In a real-world scenario, you might want to handle multiple DAGs differently.
        dag_name, dag = list(dags.items())[0]

        # Generate Python code using libcst
        # This is a placeholder and needs to be implemented
        generated_code = generate_dag_cst(dag_name, dag)
        
        print(generated_code)

    except FileNotFoundError:
        print(f"Error: Configuration file not found at {args.config_file}", file=sys.stderr)
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"Error parsing YAML file: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        sys.exit(1)

def generate_dag_cst(dag_name, dag_obj):
    # --- Imports ---
    imports = [
        cst.SimpleStatementLine(body=[cst.Import(names=[cst.ImportAlias(name=cst.Name("datetime"))])]),
        cst.SimpleStatementLine(body=[cst.ImportFrom(module=cst.Name("airflow"), names=[cst.ImportAlias(name=cst.Name("DAG"))])]),
    ]
    operator_imports = set() # To store unique operator imports

    # --- Default Arguments ---
    default_args_elements = []
    if hasattr(dag_obj, 'default_args') and dag_obj.default_args:
        for key, value in dag_obj.default_args.items():
            cst_value = _convert_to_cst_node(value) # Helper function for conversion
            if cst_value:
                default_args_elements.append(
                    cst.DictElement(_convert_to_cst_node(key), cst_value)
                )
    
    default_args_assign_node = cst.SimpleStatementLine(body=[
        cst.Assign(targets=[cst.AssignTarget(target=cst.Name("default_args"))],
                   value=cst.Dict(elements=default_args_elements))
    ]) if default_args_elements else None

    # --- DAG Instantiation ---
    dag_call_args = [
        cst.Arg(_convert_to_cst_node(dag_name)), # dag_id
        cst.Arg(value=cst.Name("default_args"), keyword=cst.Name("default_args")),
    ]
    # Add other DAG parameters dynamically
    for param_name in ['schedule_interval', 'description', 'catchup', 'tags']:
        if hasattr(dag_obj, param_name):
            param_value = getattr(dag_obj, param_name)
            if param_value is not None: # Ensure not to add if default or None
                 # For 'tags', it should be a list of strings
                if param_name == 'tags' and isinstance(param_value, list):
                    dag_call_args.append(cst.Arg(value=cst.List([_convert_to_cst_node(tag) for tag in param_value]), keyword=cst.Name(param_name)))
                elif param_name != 'tags': # handle other params
                    dag_call_args.append(cst.Arg(value=_convert_to_cst_node(param_value), keyword=cst.Name(param_name)))
    
    # --- Tasks ---
    task_nodes = []
    task_dependency_nodes = []

    if hasattr(dag_obj, 'tasks') and isinstance(dag_obj.tasks, list): # dag_obj.tasks is a list of task objects
        for task in dag_obj.tasks:
            task_id = task.task_id
            operator_class_name = task.__class__.__name__
            operator_module = task.__class__.__module__ # e.g., airflow.operators.bash

            # Add operator import
            # e.g., from airflow.operators.bash import BashOperator
            if operator_module.startswith("airflow.operators."):
                 # from airflow.operators.bash import BashOperator
                operator_import_str = f"from {operator_module} import {operator_class_name}"
                operator_imports.add(
                    cst.SimpleStatementLine(body=[
                        cst.ImportFrom(
                            module=cst.Attribute(value=cst.Attribute(value=cst.Name("airflow"), attr=cst.Name("operators")), attr=cst.Name(operator_module.split('.')[-1])), 
                            names=[cst.ImportAlias(name=cst.Name(operator_class_name))]
                        )
                    ])
                )
            elif operator_module == "airflow.sensors.external_task": # Handle ExternalTaskSensor
                operator_import_str = f"from {operator_module} import {operator_class_name}"
                operator_imports.add(
                     cst.SimpleStatementLine(body=[
                        cst.ImportFrom(
                            module=cst.Attribute(value=cst.Attribute(value=cst.Name("airflow"), attr=cst.Name("sensors")), attr=cst.Name("external_task")),
                            names=[cst.ImportAlias(name=cst.Name(operator_class_name))]
                        )
                    ])
                )


            task_params = {}
            # Extract parameters from the task object
            # This needs to be specific to how dagfactory stores operator params
            # For now, let's assume task.params or similar exists, or inspect task attributes
            # Common parameters:
            for p_name in task.params: # dagfactory tasks have a .params dict
                task_params[p_name] = task.params[p_name]

            task_call_args = [cst.Arg(value=_convert_to_cst_node(val), keyword=cst.Name(key)) for key, val in task_params.items() if key != 'task_id']
            task_call_args.insert(0, cst.Arg(value=_convert_to_cst_node(task_id), keyword=cst.Name("task_id")))
            
            task_assign = cst.SimpleStatementLine(body=[
                cst.Assign(
                    targets=[cst.AssignTarget(target=cst.Name(task_id.replace('-', '_')))], # Sanitize task_id for variable name
                    value=cst.Call(func=cst.Name(operator_class_name), args=task_call_args)
                )
            ])
            task_nodes.append(task_assign)

            # --- Task Dependencies ---
            # Handled by inspecting upstream_task_ids and downstream_task_ids
            if hasattr(task, 'upstream_task_ids') and task.upstream_task_ids:
                for up_task_id in task.upstream_task_ids:
                    dep_expr = cst.BinaryOperation(
                        left=cst.Name(up_task_id.replace('-', '_')), # Sanitize
                        operator=cst.RightShift(),
                        right=cst.Name(task_id.replace('-', '_')) # Sanitize
                    )
                    task_dependency_nodes.append(cst.SimpleStatementLine(body=[dep_expr]))
    
    # Combine unique operator imports with other imports
    all_imports = list(imports) + sorted(list(operator_imports), key=lambda x: x.body[0].module.value.value if isinstance(x.body[0], cst.ImportFrom) else x.body[0].names[0].name.value)


    with_body_elements = task_nodes
    if task_dependency_nodes: # Add a blank line before dependencies if there are tasks
        if task_nodes: with_body_elements.append(cst.EmptyLine())
        with_body_elements.extend(task_dependency_nodes)
    
    if not with_body_elements: # If no tasks, add a pass statement
        with_body_elements.append(cst.SimpleStatementLine(body=[cst.Pass()]))

    dag_definition_block = cst.With(
        items=[cst.WithItem(
            item=cst.Call(func=cst.Name("DAG"), args=dag_call_args),
            asname=cst.AsName(name=cst.Name("dag")) # `dag` variable inside the with block
        )],
        body=cst.IndentedBlock(body=with_body_elements)
    )
    
    module_body = all_imports
    if default_args_assign_node:
        module_body.extend([cst.EmptyLine(comment=cst.Comment("# Default DAG arguments")), default_args_assign_node, cst.EmptyLine()])
    else: # Add an empty line if no default_args
        module_body.append(cst.EmptyLine())
        
    module_body.extend([cst.EmptyLine(comment=cst.Comment("# DAG Definition")), dag_definition_block])
    
    module = cst.Module(body=module_body)
    return module.code

def _convert_to_cst_node(value):
    """Converts a Python value to a LibCST node."""
    if isinstance(value, str):
        # Check if it's a multiline string and use triple quotes if so
        if '\n' in value:
            return cst.SimpleString(f'"""{value}"""')
        return cst.SimpleString(f'"{value}"')
    elif isinstance(value, bool):
        return cst.Name("True") if value else cst.Name("False")
    elif isinstance(value, (int, float)):
        return cst.Integer(str(value)) if isinstance(value, int) else cst.Float(str(value))
    elif isinstance(value, list):
        return cst.List(elements=[cst.Element(_convert_to_cst_node(v)) for v in value])
    elif isinstance(value, dict):
        # Special handling for dagfactory's datetime representation
        if value.get('__type') == 'datetime':
            return cst.Call(
                func=cst.Attribute(value=cst.Name("datetime"), attr=cst.Name("datetime")),
                args=[cst.Arg(_convert_to_cst_node(arg)) for arg in value.get('args', [])]
            )
        elif value.get('__type') == 'timedelta':
             return cst.Call(
                func=cst.Attribute(value=cst.Name("datetime"), attr=cst.Name("timedelta")),
                args=[cst.Arg(_convert_to_cst_node(arg)) for arg in value.get('args', [])]
            )
        # General dictionary conversion
        return cst.Dict(elements=[cst.DictElement(_convert_to_cst_node(k), _convert_to_cst_node(v)) for k, v in value.items()])
    elif value is None:
        return cst.Name("None")
    # Add more type conversions as needed (e.g., datetime.timedelta)
    raise TypeError(f"Unsupported type for CST conversion: {type(value)} for value {value}")

if __name__ == "__main__":
    main()
