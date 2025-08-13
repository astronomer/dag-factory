# load_yaml_dags Function

The `load_yaml_dags` function is responsible for loading DAG configurations from YAML or YML files in a specified folder (or from a specific YAML file or dictionary). It parses the files or dictionary and generates Airflow DAGs by utilizing the provided globals_dict.

## Example Usage

### 1. Loading from YAML Files in a Folder

```python
from dagfactory import load_yaml_dags

# Load DAGs from a folder, e.g., /path/to/dags
load_yaml_dags(globals_dict=globals(), dags_folder='/path/to/your/dags')
```

### 2. Loading from a Specific YAML File

```python
from dagfactory import load_yaml_dags

# Load DAG from a specific YAML file
load_yaml_dags(globals_dict=globals(), config_filepath='/path/to/your/dag_config.yaml')

```

### 3. Loading from YAML File with default_args_config_path

```python
from dagfactory import load_yaml_dags

# Load a single DAG from a YAML file with custom default arguments config path
load_yaml_dags(
    globals_dict=globals(),
    config_filepath='/path/to/your/dag_config.yaml',
    default_args_config_path='/path/to/your/default_args.yml'
)
```

### 4. Loading from a Dictionary

```python
from dagfactory import load_yaml_dags

# Load DAG from a dictionary configuration
dag_config_dict = {
    # Your DAG configuration here
}
load_yaml_dags(globals_dict=globals(), config_dict=dag_config_dict)
```

### 5. Loading from a Dictionary with default_args_config_dict

```python
from dagfactory import load_yaml_dags

# Load DAGs with custom default arguments from a dictionary
default_args_dict = {
    # Your default arguments
}

dag_config_dict = {
     # Your DAG configuration here
}
load_yaml_dags(globals_dict=globals(), config_dict=dag_config_dict, default_args_config_dict=default_args_dict)
```
