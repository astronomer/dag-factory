# DAG Factory CLI documentation

After installing DAG Factory, the CLI can be invoked using the `dagfactory` command.

## Commands summary

| Command | Args   | Flags       | Description                                          |
| ------- | ------ | ----------- | ---------------------------------------------------- |
| `lint`  | `path` | `--verbose` | Check if the given directory or file is a valid YAML |

For more details about the available commands, run `dagfactory --help`.

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

### Flags

## Base command usage

```bash
dagfactory [OPTIONS]
```

### Flags

| Flag        | Alias | Description                                        |
| ----------- | ----- | -------------------------------------------------- |
| `--version` |       | Show the installed version of DAG Factory and exit |
| `--help`    | `-h`  | Show this message and exit                         |

### Version

Display the DAG Factory version:

```bash
dagfactory --version
```

Output:

```bash
DAG Factory 1.0.0a1
```

### Help

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
