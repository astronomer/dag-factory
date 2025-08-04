# DAG Factory CLI documentation

After installing DAG Factory, the CLI can be invoked using the `dagfactory` command.

## Commands summary

| Command    | Args   | Flags        | Description                                                          |
| ---------- | ------ | ------------ | -------------------------------------------------------------------- |
| `lint`     | `path` | `--verbose`  | Check if the given directory or file is a valid YAML                 |
| `convert`  | `path` | `--override` | Convert YAML file(s) from Airflow 2 to 3 in the terminal or in-place |

For more details about the available commands, run `dagfactory --help`.

## Base command usage

```bash
dagfactory [OPTIONS]
```

### Flags

| Flag        | Alias | Description                                        |
| ----------- | ----- | -------------------------------------------------- |
| `--version` |       | Show the installed version of DAG Factory and exit |
| `--help`    | `-h`  | Show this message and exit                         |

#### Identify the CLI version

Display the DAG Factory version:

```bash
dagfactory --version
```

Output:

```bash
DAG Factory 1.0.0a1
```

#### Check all the commands available in the CLI

Find out more about the DAG Factory command line:

```bash
dagfactory --help
```

Output:

```bash
Usage: dagfactory [OPTIONS]

DAG Factory: Dynamically build Apache Airflow DAGs from YAML files

Options:
  -v, --version  Show the version and exit.
  -h, --help     Show this message and exit.
```

## `lint` command

Check if the given directory contains a valid YAML files (recursively) or if the given file is a valid YAML.

### Example

```bash
 dagfactory lint some/dir --verbose
```

Output:

```bash
                           DAG Factory: YAML Lint Results
┏━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ File                 ┃ Status       ┃ Error Message                               ┃
┡━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ some/dir/v.yml       │ OK           │                                             │
├──────────────────────┼────────------┼---------------------------------────────────┤
│ some/dir/b.yaml      │ Syntax Error │ mapping values are not allowed here         │
│                      │              │   in "<unicode string>", line 2, column 7:  │
│                      │              │       host: localhost                       │
│                      │              │           ^                                 │
├──────────────────────┼──────────────┼─────────────────────────────────────────────┤
│ some/dir/a.yml       │ Syntax Error │ while parsing a flow sequence               │
│                      │              │   in "<unicode string>", line 2, column 5:  │
│                      │              │       - [orange, mango                      │
│                      │              │         ^                                   │
│                      │              │ expected ',' or ']', but got '<stream end>' │
│                      │              │   in "<unicode string>", line 3, column 1:  │
│                      │              │                                             │
│                      │              │     ^                                       │
└──────────────────────┴──────────────┴─────────────────────────────────────────────┘
Analysed 3 files, found 2 invalid YAML files.
```

## `convert`  command

Given a path to either a directory containing YAML files or to a path to a single YAML file, tries to convert them from Airflow 2 to 3. By default, displays the necessary changes in the terminal (default). If using the flag `--override`, changes the original files with the necessary changes.

### Example

```bash
 dagfactory convert dev/dags/airflow3
```

Output:

```bash
No changes needed: dev/dags/airflow3/example_params.yml
No changes needed: dev/dags/airflow3/example_dag_factory_multiple_config.yml
No changes needed: dev/dags/airflow3/example_task_group.yml
No changes needed: dev/dags/airflow3/example_dag_factory_default_args.yml
─────────────────────────────────────────────────── Diff for dev/dags/airflow3/example_customize_operator.yml ───────────────────────────────────────────────────
--- dev/dags/airflow3/example_customize_operator.yml
+++ dev/dags/airflow3/example_customize_operator.yml (converted)
@@ -11,7 +11,7 @@
   schedule: 0 3 * * *
   tasks:
   - task_id: begin
-    operator: airflow.operators.empty.EmptyOperator
+    operator: airflow.providers.standard.operators.empty.EmptyOperator
   - task_id: make_bread_1
     operator: customized.operators.breakfast_operators.MakeBreadOperator
     bread_type: Sourdough
@@ -30,7 +30,7 @@
     - make_bread_1
     - make_bread_2
   - task_id: end
-    operator: airflow.operators.empty.EmptyOperator
+    operator: airflow.providers.standard.operators.empty.EmptyOperator
     dependencies:
     - begin
     - make_bread_1
No changes needed: dev/dags/airflow3/example_custom_py_object_dag.yml
No changes needed: dev/dags/airflow3/example_taskflow.yml
No changes needed: dev/dags/airflow3/example_jinja2_template_dag.yml
No changes needed: dev/dags/airflow3/example_dag_factory_default_config.yml
No changes needed: dev/dags/airflow3/example_dynamic_task_mapping.yml
Tried to convert 10 files, converted 1 file, no errors found.
```
