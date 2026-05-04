# DAG Factory CLI documentation

After installing DAG Factory, the CLI can be invoked using the `dagfactory` command.

## Commands summary

| Command   | Description                                                          |
| --------- | -------------------------------------------------------------------- |
| `lint`    | Validate dag-factory loader / YAML files against the bundled schema  |
| `convert` | Convert YAML file(s) from Airflow 2 to 3 in the terminal or in-place |

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

```bash
dagfactory --version
```

## `lint` command

Validate DAG parameters end-to-end. The lint command accepts three input modes and two validation strategies.

### Input modes

| Input                | Example                                                  | What is validated                                                                                                                                |
| -------------------- | -------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| Python loader (`.py`) | `dagfactory lint dags/loader.py`                         | The loader is imported and every `load_yaml_dags(...)` invocation is captured. dag-factory's own defaults handling (defaults.yml chain, `defaults_config_dict`, etc.) runs end-to-end. |
| YAML file (`.yml`/`.yaml`) | `dagfactory lint dags/my_dag.yml`                  | Each top-level DAG entry is validated as a self-contained config. The file's own `default:` block is applied; no external defaults are merged.     |
| Inline YAML          | `dagfactory lint --yaml-content "$(cat my_dag.yml)"`     | Same semantics as a YAML file, supplied as a string. Useful for editor / IDE integration.                                                          |

When a directory is passed, the walker finds all `.py` files that import `dagfactory` and lints them. Files named `defaults.yml`/`defaults.yaml` are recognised as dag-factory infrastructure and skipped with a warning. Pass `--lint-yaml-in-dir` to also include `.yml`/`.yaml` files alongside the loaders, while this should be an uncommon case, as the .py files should cover all DAGs already.

### Validation strategies

| Strategy                       | Behaviour                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| ------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Build mode (default)**       | Runs the full dag-factory + Airflow pipeline. Catches operator typos, missing required args, dependency cycles, conflicting schedules, bad date strings — anything that would fail at DAG-build time. Requires every operator package referenced in the YAML to be importable in the lint environment.                                                                                                                                                              |
| **Schema mode (`--schema-only`)** | Intercepts dag-factory before any DAG is built and validates the resolved configs against the bundled JSON schema. Cheaper and runs cleanly in environments that don't have every operator installed. Detects schema-level issues only: removed-in-AF3 fields, deprecated parameters, missing required fields, type mismatches, etc.                                                                                                                                  |

### Examples

Lint a single Python loader (full build):

```bash
dagfactory lint dev/dags/airflow3/example_dag_factory.py
```

Lint a YAML config against Airflow 2 (schema-only):

```bash
dagfactory lint --schema-only --airflow-version 2 dev/dags/airflow2/example_params.yml
```

Lint inline YAML supplied from a script or editor:

```bash
dagfactory lint --schema-only --yaml-content "$(cat my_dag.yml)"
```

Lint everything under a folder, including the YAML files (Every DAG would be linted twice, one with .py file and the other one with YAML file):

```bash
dagfactory lint --lint-yaml-in-dir dev/dags/airflow3
```

## Using the JSON schema in your IDE

The JSON schema that powers `dagfactory lint --schema-only` is bundled as a regular JSON file and can be wired into any editor that supports JSON Schema for YAML validation (VS Code via the YAML extension, JetBrains IDEs natively, Neovim with `yaml-language-server`, etc.). This gives you interactive feedback on dag-factory YAML files while you type — no Python toolchain in the loop.

The schema lives at `dagfactory/schemas/dag_parameters.json` in the installed package. To find its absolute path on your machine:

```bash
python -c "from importlib.resources import files; print(files('dagfactory.schemas') / 'dag_parameters.json')"
```

### VS Code (YAML extension)

Add to `.vscode/settings.json`:

```json
{
  "yaml.schemas": {
    "/absolute/path/to/dagfactory/schemas/dag_parameters.json": [
      "dags/**/*.yml",
      "dags/**/*.yaml"
    ]
  }
}
```

### JetBrains IDEs (PyCharm, IntelliJ, etc.)

Settings → Languages & Frameworks → Schemas and DTDs → JSON Schema Mappings → add a mapping from the schema file to your DAG YAML directory.

### Notes on standalone use

The standalone schema validates the static structure of a YAML file: required fields, value types, removed/deprecated parameters, and dag-factory-specific conventions. It does **not** apply external defaults (`defaults.yml`, `defaults_config_dict`) or run dag-factory's loader, so cross-file constraints and operator-import errors are only caught by `dagfactory lint`.

## `convert`  command

Given a path to either a directory containing YAML files or to a path to a single YAML file, tries to convert them from Airflow 2 to 3. By default, displays the necessary changes in the terminal (default). If using the flag `--override`, changes the original files with the necessary changes.

### Example

```bash
 dagfactory convert dev/dags/airflow3
```

Output:

```bash
No changes needed: dev/dags/airflow3/example_params.yml
─────────────────────────────────────────────────── Diff for dev/dags/airflow3/example_customize_operator.yml ───────────────────────────────────────────────────
--- dev/dags/airflow3/example_customize_operator.yml
+++ dev/dags/airflow3/example_customize_operator.yml (converted)
@@ -11,7 +11,7 @@
   schedule: 0 3 * * *
   tasks:
   - task_id: begin
-    operator: airflow.operators.empty.EmptyOperator
+    operator: airflow.providers.standard.operators.empty.EmptyOperator
Tried to convert 10 files, converted 1 file, no errors found.
```
