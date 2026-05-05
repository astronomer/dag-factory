# load_yaml_dags Function

The `load_yaml_dags` function loads DAG configurations from YAML/YML files in a folder, from a specific YAML file, or from a Python dictionary. It parses the config and generates Airflow DAGs with the provided `globals_dict`.

## Example Usage

### 1. Loading from YAML Files in a Folder

```python
from dagfactory import load_yaml_dags

# Load DAGs from a folder, e.g. /path/to/dags
load_yaml_dags(globals_dict=globals(), dags_folder="/path/to/your/dags")
```

### 2. Loading from a Specific YAML File

```python
from dagfactory import load_yaml_dags

# Load DAGs from a specific YAML file
load_yaml_dags(globals_dict=globals(), config_filepath="/path/to/your/dag_config.yaml")
```

### 3. Loading from a YAML File with `defaults_config_path`

```python
from dagfactory import load_yaml_dags

# Load a single DAG from a YAML file with a custom defaults root directory
load_yaml_dags(
    globals_dict=globals(),
    config_filepath="/path/to/your/dag_config.yaml",
    defaults_config_path="/path/to/your/config-root",
)
```

`defaults_config_path` should point to a directory root that contains one or more
`defaults.yml` or `defaults.yaml` files, not to an individual defaults file.

### 4. Loading from a Dictionary

```python
from dagfactory import load_yaml_dags

# Load DAGs from a dictionary configuration
dag_config_dict = {
    # Your DAG configuration here
}
load_yaml_dags(globals_dict=globals(), config_dict=dag_config_dict)
```

### 5. Loading from a Dictionary with `defaults_config_dict`

```python
from dagfactory import load_yaml_dags

# Load DAGs with custom default arguments from a dictionary
default_args_dict = {
    # Your default arguments
}

dag_config_dict = {
    # Your DAG configuration here
}
load_yaml_dags(
    globals_dict=globals(),
    config_dict=dag_config_dict,
    defaults_config_dict=default_args_dict,
)
```

## `.airflowignore` Support

When `load_yaml_dags` scans a `dags_folder`, it reads `.airflowignore` files in that folder tree and skips matching YAML files.

- Matching syntax follows Airflow's `core.dag_ignore_file_syntax` setting.
- In `glob` mode, patterns use gitignore-style `gitwildmatch` semantics, matching Airflow more closely.
- In `regexp` mode, each non-comment line is treated as a regular expression matched against the DAG-root-relative path.
- Empty lines are ignored.
- `#` starts a comment for the rest of the line, so inline comments are stripped before matching.
- In `glob` mode, patterns without `/` match matching basenames anywhere in the scoped subtree, not just at the root.
- In `glob` mode, patterns ending in `/` ignore matching directories and everything beneath them.
- In `glob` mode, negation patterns beginning with `!` re-include matching files, with later matches overriding earlier ones.
- In `glob` mode, patterns with `/` are matched against the POSIX-style path relative to the directory that contains the matching `.airflowignore`.
- Nested `.airflowignore` files apply to files in their own directory subtree and can override parent rules, unless a parent rule already prunes that subtree from traversal.
- Symlinked directories are traversed for both YAML discovery and nested `.airflowignore` lookup.
- Lexically in-tree symlinked files still participate in path-based matching.
- For files outside `dags_folder`, only root-level basename patterns are considered in `glob` mode; scoped path rules do not apply.

### Example `.airflowignore`

```text
*.yml
!keep.yml
ignored/
backup/**/*.yaml
```

With this file:

- `nested/test_example.yml` is ignored because basename patterns apply throughout the subtree.
- `keep.yml` is **not** ignored because `!keep.yml` overrides the earlier `*.yml` match.
- `ignored/dag.yml` is ignored.
- `nested/ignored/dag.yml` is also ignored by `ignored/`.
- `backup/file.yaml` is ignored.
- `backup/subdir/file.yaml` is ignored.
- `backup2/file.yaml` is **not** ignored by `backup/**/*.yaml`.

### Example

```python
from dagfactory import load_yaml_dags

load_yaml_dags(
    globals_dict=globals(),
    dags_folder="/path/to/your/dags",
)
```

If `/path/to/your/dags/.airflowignore` exists, matching YAML files are skipped during discovery.
