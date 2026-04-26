import difflib
from copy import deepcopy
from pathlib import Path
from typing import Optional

import typer
import yaml
from airflow.version import version as AIRFLOW_VERSION
from rich.console import Console
from rich.table import Table
from rich.text import Text

from dagfactory import __version__
from dagfactory._yaml import load_yaml_file
from dagfactory.utils import update_yaml_structure
from dagfactory.validator import DagParameterValidator, imports_dagfactory

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


def _find_lintable_files(path: Path, lint_yaml_in_dir: bool = False) -> list[Path]:
    """Find files lint can process under *path*.

    For a single file target, it's lintable if the file suffix is .py or .yml/.yaml.

    For a directory target, .py loaders are always discovered. YAML files
    are only included when lint_yaml_in_dir is True.
    """
    if not path.exists():
        console.print(f"[red]Error:[/red] Path '{path}' does not exist.")
        raise typer.Exit(1)

    if path.is_dir():
        py_candidates = list(path.rglob("*.py"))
        py_loaders = [p for p in py_candidates if imports_dagfactory(p)]
        if lint_yaml_in_dir:
            yaml_files = list(path.rglob("*.yml")) + list(path.rglob("*.yaml"))
            files = py_loaders + yaml_files
        else:
            files = py_loaders
        if not files:
            extra = "" if lint_yaml_in_dir else " (pass --lint-yaml-in-dir to also include .yml/.yaml files)"
            console.print(
                f"[yellow]No lintable files found in '{path}' "
                f"({len(py_candidates)} .py file(s) scanned; none import dagfactory).{extra}[/yellow]"
            )
            raise typer.Exit()
        return files

    if path.suffix == ".py":
        if not imports_dagfactory(path):
            console.print(
                f"[yellow]'{path}' does not import dagfactory; skipping (not a loader file).[/yellow]"
            )
            raise typer.Exit()
        return [path]

    if path.suffix in (".yml", ".yaml"):
        return [path]

    console.print(
        f"[red]Error:[/red] lint operates on .py loader files or .yml/.yaml configs; "
        f"got '{path.suffix}'."
    )
    raise typer.Exit(1)


def _find_yaml_files(path: Path) -> list[Path]:
    """YAML-only file finder retained for the ``convert`` command."""
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
    path: Optional[Path] = typer.Argument(
        None,
        help="Path to a Python loader (.py) file, a YAML config (.yml/.yaml) file, "
        "or a directory containing either. External defaults are not supported when linting YAML files.",
    ),
    yaml_content: Optional[str] = typer.Option(
        None,
        "--yaml-content",
        "-c",
        help="Inline YAML content to validate (mutually exclusive with the path argument).",
    ),
    airflow_version: str = typer.Option(
        AIRFLOW_VERSION,
        "--airflow-version",
        "-a",
        help="Airflow version to validate against (e.g. '3.1.2' or just '2'). "
        "Defaults to the installed Airflow version.",
    ),
    schema_only: bool = typer.Option(
        False,
        "--schema-only",
        help="Validate only against the bundled JSON schema; do not build real Airflow DAGs. "
        "Useful when not every operator package referenced in the YAML is installed.",
    ),
    lint_yaml_in_dir: bool = typer.Option(
        False,
        "--lint-yaml-in-dir",
        help="When the path argument is a directory, also lint .yml/.yaml files alongside .py "
        "loaders. No effect on single-file or --yaml-content invocations.",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show full error messages"),
):
    """Validate dag-factory loaders and YAML configs.

    Default (build mode) runs the full dag-factory + Airflow build pipeline and reports
    any exception (operator typos, dependency cycles, schedule conflicts, etc.).
    Pass --schema-only to validate against the bundled JSON schema instead — cheaper,
    and works without every operator package installed.
    """
    if (path is None) == (yaml_content is None):
        console.print(
            "[red]Error:[/red] provide either a path argument or --yaml-content (not both)."
        )
        raise typer.Exit(1)

    table = Table(
        title="[bold][medium_purple3]DAG Factory[/medium_purple3][/bold]: Lint Results",
        show_lines=True,
    )
    table.add_column("File", style="cyan", no_wrap=True)
    table.add_column("Status", style="bold")
    table.add_column("Error Message", style="red", no_wrap=False, overflow="fold")

    validator = DagParameterValidator(airflow_version=airflow_version, schema_only=schema_only)

    total_errors = 0
    total_warnings = 0

    def render_results(label_prefix, results):
        """Render a list of FileValidationResult into the table."""
        nonlocal total_errors, total_warnings
        for sub in results:
            if label_prefix and sub.file != Path(label_prefix):
                label = f"{label_prefix} → {sub.file.name}"
            else:
                label = str(sub.file)
            if sub.errors:
                total_errors += 1
                total_warnings += len(sub.warnings)
                msg = _format_issues(sub, verbose)
                table.add_row(label, Text("Error", style="red"), Text(msg, style="red"))
            elif sub.warnings:
                total_warnings += len(sub.warnings)
                msg = _format_issues(sub, verbose)
                table.add_row(label, Text("Warnings", style="yellow"), Text(msg, style="yellow"))
            else:
                table.add_row(label, Text("OK", style="green"), "")

    if yaml_content is not None:
        results = validator.validate_yaml_content(yaml_content)
        render_results(label_prefix=None, results=results)
        analysed = 1
    else:
        files = _find_lintable_files(path, lint_yaml_in_dir=lint_yaml_in_dir)
        for file_path in files:
            if file_path.suffix == ".py":
                results = validator.validate_python_loader(file_path)
            else:  # .yml / .yaml
                results = validator.validate_yaml_file(file_path)
            render_results(label_prefix=str(file_path), results=results)
        analysed = len(files)

    console.print(table)
    summary = f"Analysed {analysed} file(s)"
    if total_errors:
        console.print(
            f"{summary}, found [red]{total_errors}[/red] file(s) with errors and "
            f"[yellow]{total_warnings}[/yellow] warning(s)."
        )
        if not verbose:
            console.print(f"For more details on the errors, run with --verbose.")
        raise typer.Exit(1)
    if total_warnings:
        console.print(
            f"{summary}, [green]no errors[/green], [yellow]{total_warnings}[/yellow] warning(s)."
        )
        return
    console.print(f"{summary}, [green]no errors found.[/green]")


def _format_issues(result, verbose: bool) -> str:
    """Format validator findings for display in the lint table."""
    lines = [f"{i.severity.upper()}: {i.render()}" for i in result.issues]
    text = "\n".join(lines)
    if verbose:
        return text
    if len(text) > 200:
        return text[:197] + "..."
    return text


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
