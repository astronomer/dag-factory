# DAG Factory CLI documentation

After installing DAG Factory, the CLI can be invoked using the `dagfactory` command.

## Usage

```bash
dagfactory [OPTIONS]
```

| Option      | Alias | Description                                        |
| ----------- | ----- | -------------------------------------------------- |
| `--version` | `-v`  | Show the installed version of DAG Factory and exit |
| `--help`    | `-h`  | Show this message and exit                         |

## Version

Display the DAG Factory version:

```bash
dagfactory --version
```

Output:

```bash
DAG Factory 1.0.0a1
```

## Help

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
