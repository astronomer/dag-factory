import difflib
from copy import deepcopy
from pathlib import Path
from typing import Optional

import typer
import yaml
from airflow.version import version as AIRFLOW_VERSION
from packaging.version import InvalidVersion, Version
from rich.console import Console
from rich.table import Table
from rich.text import Text

from dagfactory import __version__
from dagfactory._yaml import load_yaml_file
from dagfactory.constants import DEFAULTS_FILE_NAMES
from dagfactory.lint import lint_file
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


def _load_error(file_path: Path) -> Optional[Exception]:
    """Return whatever stopped the YAML file loading, or None if it loaded.

    Loading also materialises `__type__` directives, so this catches more than
    a syntax error: anything the import raises lands here too. Returning the
    exception rather than a string leaves the caller to decide how to show it.
    """
    try:
        load_yaml_file(file_path)
    except Exception as exc:
        return exc
    return None


def _describe(exc: Exception, verbose: bool) -> str:
    """Render a load failure for the results table."""
    text = str(exc).strip() if isinstance(exc, yaml.YAMLError) else f"{type(exc).__name__}: {exc}"
    if verbose:
        return text
    first_line = text.split("\n")[0]
    return first_line[:120] + "..."


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


def _find_yaml_files_on_airflow_ignore(path: Path) -> list[Path] | None:
    """
    Find YAML files on airflowignore.
    """
    airflow_ignore = next(path.rglob(".airflowignore"), None)
    if not airflow_ignore:
        return airflow_ignore

    with open(airflow_ignore, "r") as f:
        files = [path / Path(line.strip()) for line in f if line.strip() and not line.startswith("#")]
        if not files:
            return None
    return files


def _exclude_yaml_files(files: list[Path], ignore: Path, airflow_ignore: list[Path] | None):
    """
    Exclude files and directories from the list of YAML files.
    """
    if "," in str(ignore):
        ignore_files = [Path(p.strip()) for p in str(ignore).split(",")]
    else:
        ignore_files = [ignore]

    ignore_paths = set()
    for ignore_path in ignore_files:
        if not ignore_path.exists():
            console.print(f"Ignore path '{ignore_path}' does not exist, skipping.")
            continue

        if ignore_path.is_dir():
            ignore_paths.update(list(ignore_path.rglob("*.yaml")) + list(ignore_path.rglob("*.yml")))
        else:
            ignore_paths.add(ignore_path)

    if airflow_ignore:
        for ignore_path in airflow_ignore:
            if ignore_path.is_dir():
                ignore_paths.update(list(ignore_path.rglob("*.yaml")) + list(ignore_path.rglob("*.yml")))
                continue

            file_without_suffix = ignore_path.with_suffix("")
            ignore_file = next(ignore_path.parent.rglob(file_without_suffix.name + ".yaml"), None) or next(
                ignore_path.parent.rglob(file_without_suffix.name + ".yml"), None
            )
            if ignore_file:
                ignore_paths.add(ignore_file)

    initial_count = len(files)
    files[:] = [f for f in files if f not in ignore_paths]
    excluded_count = initial_count - len(files)

    if excluded_count > 0:
        console.print(
            f"[blue]Ignored {excluded_count} YAML {_file_or_files(excluded_count)} based on --ignore option.[/blue]"
        )


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
    ignore: Path = typer.Option(None, "--ignore", "-i", help="Files or directories to ignore"),
    airflow_version: str = typer.Option(
        AIRFLOW_VERSION,
        "--airflow-version",
        "-a",
        help="Airflow version to check against (e.g. '3.1.2' or just '2'). "
        "Defaults to the installed Airflow version.",
    ),
    check_operators: bool = typer.Option(
        True,
        "--check-operators/--no-check-operators",
        help="Import each task's operator to confirm it exists. Turn it off to lint where the "
        "provider packages are not installed. Other imports may still happen while resolving a "
        "config, such as callables and `__type__` directives.",
    ),
    defaults_path: Optional[Path] = typer.Option(
        None,
        "--defaults-path",
        help="Root directory to search for defaults.yml/defaults.yaml, as dag-factory does at "
        "runtime. Defaults to Airflow's dags_folder.",
    ),
):
    """Scan YAML configs for syntax errors and invalid DAG parameters.

    Each DAG is resolved the way the runtime resolves it, including the
    defaults.yml chain, then checked against the parameter metadata: unknown
    keys, wrong types, parameters the configured Airflow does not accept,
    deprecated parameters, and missing required fields.

    Files named defaults.yml / defaults.yaml are dag-factory infrastructure
    rather than DAG configs, so they are not linted in their own right; their
    contents still reach the DAGs that inherit them.
    """
    try:
        target_version = Version(str(airflow_version))
    except InvalidVersion:
        console.print(f"[red]Error:[/red] --airflow-version must be a PEP440 version, got {airflow_version!r}.")
        raise typer.Exit(2)

    files = [f for f in _find_yaml_files(path) if f.name not in DEFAULTS_FILE_NAMES]
    airflow_ignore_files = _find_yaml_files_on_airflow_ignore(path)
    if ignore:
        _exclude_yaml_files(files, ignore, airflow_ignore_files)

    table = Table(title="[bold][medium_purple3]DAG Factory[/medium_purple3][/bold]: Lint Results", show_lines=True)
    table.add_column("File", style="cyan", no_wrap=True)
    table.add_column("Status", style="bold")
    table.add_column("Error Message", style="red", no_wrap=False, overflow="fold")

    total_errors = 0
    total_warnings = 0
    for file_path in files:
        load_error = _load_error(file_path)
        if load_error is not None:
            total_errors += 1
            table.add_row(
                str(file_path),
                Text("Syntax Error", style="red"),
                Text(_describe(load_error, verbose), style="red"),
            )
            continue

        result = lint_file(
            file_path,
            target_version,
            str(defaults_path) if defaults_path else None,
            check_operators=check_operators,
        )
        if result.errors:
            total_errors += 1
            total_warnings += len(result.warnings)
            table.add_row(
                str(file_path), Text("Error", style="red"), Text(_format_findings(result, verbose), style="red")
            )
        elif result.warnings:
            total_warnings += len(result.warnings)
            table.add_row(
                str(file_path),
                Text("Warnings", style="yellow"),
                Text(_format_findings(result, verbose), style="yellow"),
            )
        else:
            table.add_row(str(file_path), Text("OK", style="green"), "")

    console.print(table)
    summary = f"Analysed {len(files)} {_file_or_files(len(files))}"
    if total_errors:
        console.print(f"{summary}, found [red]{total_errors}[/red] with errors and {total_warnings} warning(s).")
        if not verbose:
            console.print("For more details on the errors, run with --verbose.")
        raise typer.Exit(1)
    if total_warnings:
        console.print(f"{summary}, [green]no errors found[/green] ([yellow]{total_warnings} warning(s)[/yellow]).")
    else:
        console.print(f"{summary}, [green]no errors found.[/green]")


def _format_findings(result, verbose: bool) -> str:
    """Render a file's findings for the results table."""
    shown = result.errors + result.warnings
    if not verbose:
        shown = shown[:3]
    lines = [f"{'error' if f.severity == 'error' else 'warn'}: {f.render()}" for f in shown]
    hidden = len(result.errors) + len(result.warnings) - len(shown)
    if hidden > 0:
        lines.append(f"... and {hidden} more; run with --verbose")
    return "\n".join(lines)


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


if __name__ == "__main__":  # pragma: no cover
    app()
