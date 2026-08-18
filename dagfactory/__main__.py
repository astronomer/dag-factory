import difflib
from copy import deepcopy
from pathlib import Path

import typer
import yaml
from rich.console import Console
from rich.table import Table
from rich.text import Text

from dagfactory import __version__
from dagfactory._yaml import load_yaml_file
from dagfactory.constants import DEFAULTS_FILE_NAMES
from dagfactory.dag_codegen import generate_dag_block, generate_dags_file
from dagfactory.dagfactory import _DagFactory
from dagfactory.utils import update_yaml_structure

DESCRIPTION = """
[bold][medium_purple3]DAG Factory[/medium_purple3][/bold]: Dynamically build Apache Airflow DAGs from YAML files

Find out more at: https://github.com/astronomer/dagfactory
"""


console = Console()


app = typer.Typer(
    name="dagfactory",
    context_settings={"help_option_names": ["-h", "--help"]},
    invoke_without_command=True,
)


def _check_yaml_syntax(file_path: Path):
    """
    Check if the YAML file is valid.
    """
    try:
        load_yaml_file(file_path)
    except yaml.YAMLError as e:
        return str(e)


def _find_yaml_files(path: Path) -> list[Path]:
    """
    Find all YAML files in the directory.
    """
    if not path.exists():
        console.print(f"[red]Error:[/red] Path '{path}' does not exist.")
        raise typer.Exit(1)

    if path.is_dir():
        files = list(path.rglob("*.yaml")) + list(path.rglob("*.yml"))
    else:
        files = [path]

    if not files:
        console.print(f"[yellow]No YAML files found in '{path}'.[/yellow]")
        raise typer.Exit()

    return files


@app.callback()
def main(
    ctx: typer.Context,
    version: bool = typer.Option(
        None,
        "--version",
        help="Show the version and exit.",
        is_eager=True,  # Display version immediately before parsing other options
    ),
):
    if version:
        console.print(f"DAG Factory {__version__}")
        raise typer.Exit()

    if ctx.invoked_subcommand is None:
        console.print(DESCRIPTION)
        typer.echo(ctx.get_help())


@app.command()
def lint(
    path: Path = typer.Argument(..., help="Path to a directory containing YAML files or to a YAML file to lint"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show full error messages"),
):
    """Scan YAML files for syntax errors."""
    files = _find_yaml_files(path)

    table = Table(title="[bold][medium_purple3]DAG Factory[/medium_purple3][/bold]: YAML Lint Results", show_lines=True)
    table.add_column("File", style="cyan", no_wrap=True)
    table.add_column("Status", style="bold")
    table.add_column("Error Message", style="red", no_wrap=False, overflow="fold")

    total_errors = 0
    for file_path in files:
        error = _check_yaml_syntax(file_path)
        if error:
            total_errors += 1
            message = error.strip() if verbose else error.strip().split("\n")[0][:120] + "..."
            table.add_row(str(file_path), Text("Syntax Error", style="red"), Text(message, style="red"))
        else:
            table.add_row(str(file_path), Text("OK", style="green"), "")

    console.print(table)
    if total_errors > 0:
        console.print(f"Analysed {len(files)} files, found [red]{total_errors}[/red] invalid YAML files.")
        if not verbose:
            console.print(f"For more details on the errors, run with --verbose.")
        raise typer.Exit(1)
    else:
        console.print(f"Analysed {len(files)} files, [green]no errors found.[/green]")


def _file_or_files(count: int) -> str:
    """
    Return 'file' if the count is 1, otherwise return 'files'.
    """
    if count == 1:
        return "file"
    else:
        return "files"


@app.command()
def convert(
    path: Path = typer.Argument(..., help="Path to a YAML file or a directory of YAML files to convert"),
    # type: str = typer.Option("airflow2to3", "--type", "-t", help="Conversion type (default: airflow2to3)"),
    override: bool = typer.Option(False, "--override", "-o", help="Write the converted YAML back to file"),
):
    """Convert YAML files from Airflow 2 to 3 in the terminal or in-place."""
    files = _find_yaml_files(path)
    total_errors = 0
    total_converted = 0

    for file in files:
        try:
            original_data = load_yaml_file(file)
            # we need to create a copy because the `update_yaml_structure` modifies the content by reference
            converted_data = update_yaml_structure(deepcopy(original_data))

            original_yaml = yaml.dump(original_data, sort_keys=False)
            converted_yaml = yaml.dump(converted_data, sort_keys=False)

            if original_data != converted_data:
                total_converted += 1
                if override:
                    file.write_text(converted_yaml)
                    console.print(f"[green]✓ Converted:[/green] {file}")
                else:
                    diff_lines = list(
                        difflib.unified_diff(
                            original_yaml.splitlines(),
                            converted_yaml.splitlines(),
                            fromfile=str(file),
                            tofile=str(file) + " (converted)",
                            lineterm="",
                        )
                    )

                    if diff_lines:
                        console.rule(f"[bold blue]Diff for {file}")
                        for line in diff_lines:
                            if line.startswith("+") and not line.startswith("+++"):
                                console.print(Text(line, style="green"))
                            elif line.startswith("-") and not line.startswith("---"):
                                console.print(Text(line, style="red"))
                            else:
                                console.print(line)
            else:
                console.print(f"[blue]No changes needed:[/blue] {file}")

        except Exception as e:
            total_errors += 1
            console.print(f"[red]Failed to convert {file}:[/red] {str(e)}")

    if total_errors:
        console.print(
            f"Tried to convert {len(files)} {_file_or_files(len(files))}, converted [green]{total_converted}[/green] {_file_or_files(total_converted)}, found [red]{total_errors}[/red] invalid YAML {_file_or_files(total_errors)}."
        )
        raise typer.Exit(1)
    else:
        console.print(
            f"Tried to convert {len(files)} {_file_or_files(len(files))}, converted [green]{total_converted}[/green] {_file_or_files(total_converted)}, [green]no errors found.[/green]"
        )


@app.command()
def generate(
    yaml_file_dir: Path = typer.Argument(..., help="Path to a directory containing YAML files to generate DAGs from"),
    py_dags_dir: Path = typer.Argument(
        ..., help="Path to a directory where the generated .py DAG files will be written"
    ),
):

    if not yaml_file_dir.exists():
        console.print(f"[red]Error:[/red] Path '{yaml_file_dir}' does not exist.")
        raise typer.Exit(1)

    if not py_dags_dir.exists():
        py_dags_dir.mkdir(parents=True)

    all_yaml_files = list(yaml_file_dir.rglob("*.yaml")) + list(yaml_file_dir.rglob("*.yml"))
    # `defaults.yml`/`defaults.yaml` files hold shared default_args for other DAGs in the
    # directory tree — they are not DAG definitions themselves and must not be generated as one.
    yaml_files = [f for f in all_yaml_files if f.name not in DEFAULTS_FILE_NAMES]
    if not yaml_files:
        console.print(f"[yellow]No YAML files found in '{yaml_file_dir}'.[/yellow]")
        raise typer.Exit(0)

    errors = []
    skipped = []
    for yaml_file in yaml_files:
        try:
            # `cast_types=False` keeps `__type__` dicts as-is, so `dag_codegen` can re-emit them
            # as real constructor source code instead of an already-instantiated, unrenderable object.
            config = load_yaml_file(str(yaml_file), cast_types=False)
            default_config = config.get("default", {})

            # Merge in the shared `defaults.yml`, if any, the same way the runtime YAML loader does:
            # global default_args are lowest priority, this file's own `default:` args take precedence.
            factory = _DagFactory(
                config_filepath=str(yaml_file.resolve()), defaults_config_path=str(yaml_file_dir.resolve())
            )
            global_default_args = factory._global_default_args()
            dag_level_args = {}
            if isinstance(global_default_args, dict):
                default_config["default_args"] = factory._merge_default_args_from_list_configs(
                    [global_default_args, default_config]
                )
                dag_level_args = factory._merge_dag_args_from_list_configs([global_default_args])

            dags_to_generate = {}
            for dag_name in config:
                if dag_name == "default":
                    continue
                if not isinstance(config[dag_name], dict):
                    continue
                dag_config = {**dag_level_args, **deepcopy(config[dag_name])}
                for key, value in default_config.items():
                    if key not in dag_config:
                        dag_config[key] = deepcopy(value)
                try:
                    generate_dag_block(dag_name, dag_config)  # validate first
                    dags_to_generate[dag_name] = dag_config
                    console.print(f"[green]✓ DAG {dag_name} generated successfully")
                except ValueError as e:
                    console.print(
                        f"[yellow]⚠ Skipping DAG '{dag_name}' in '{yaml_file.name}': {e} "
                        f"— tasks may be inheriting config not supported by generate[/yellow]"
                    )
                    skipped.append(f"{yaml_file.name}::{dag_name}")
            if dags_to_generate:
                py_file = py_dags_dir / (yaml_file.stem + ".py")
                py_file.write_text(generate_dags_file(dags_to_generate))
        except yaml.YAMLError as e:
            console.print(f"[yellow]⚠ Skipping '{yaml_file.name}': invalid YAML syntax — {e}[/yellow]")
            skipped.append(yaml_file.name)
        except Exception as e:
            error_msg = str(e)
            if "No module named" in error_msg:
                import re

                match = re.search(r"No module named '([^']+)'", error_msg)
                package = match.group(1) if match else "unknown"
                console.print(
                    f"[yellow]⚠ Skipping '{yaml_file.name}': missing optional package '{package}'. "
                    f"Install it with: pip install {package}[/yellow]"
                )
                skipped.append(yaml_file.name)
            else:
                console.print(f"[red]✗ Skipping '{yaml_file.name}': {e}[/red]")
                errors.append(yaml_file)

    if skipped:
        console.print(
            f"[yellow]{len(skipped)} DAG(s)/file(s) were skipped and NOT generated — "
            f"see warnings above. Failing so this isn't silently missed in CI.[/yellow]"
        )
    if errors or skipped:
        raise typer.Exit(1)


if __name__ == "__main__":  # pragma: no cover
    app()
