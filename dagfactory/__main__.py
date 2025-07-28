from pathlib import Path

import typer
import yaml
from rich.console import Console
from rich.table import Table
from rich.text import Text

from dagfactory import __version__
from dagfactory._yaml import load_yaml_file

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


def check_yaml_syntax(file_path: Path):
    """
    Check if the YAML file is valid.
    """
    try:
        load_yaml_file(file_path)
    except yaml.YAMLError as e:
        return str(e)


def find_yaml_files(directory: Path):
    """
    Find all YAML files in the directory.
    """
    return list(directory.rglob("*.yaml")) + list(directory.rglob("*.yml"))


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
    if not path.exists():
        console.print(f"[red]Error:[/red] Path '{path}' does not exist.")
        raise typer.Exit(1)

    if path.is_dir():
        files = find_yaml_files(path)
    else:
        files = [path]

    if not files:
        console.print(f"[yellow]No YAML files found in '{path}'.[/yellow]")
        raise typer.Exit()

    table = Table(title="[bold][medium_purple3]DAG Factory[/medium_purple3][/bold]: YAML Lint Results", show_lines=True)
    table.add_column("File", style="cyan", no_wrap=True)
    table.add_column("Status", style="bold")
    table.add_column("Error Message", style="red", no_wrap=False, overflow="fold")

    total_errors = 0
    for file_path in files:
        error = check_yaml_syntax(file_path)
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


if __name__ == "__main__":  # pragma: no cover
    app()
