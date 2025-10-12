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
):
    """Scan YAML files for syntax errors."""
    files = _find_yaml_files(path)
    airflow_ignore_files = _find_yaml_files_on_airflow_ignore(path)
    if ignore:
        _exclude_yaml_files(files, ignore, airflow_ignore_files)

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


if __name__ == "__main__":  # pragma: no cover
    app()
