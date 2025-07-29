import typer
from rich.console import Console

from . import __version__

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


@app.callback()
def main(
    ctx: typer.Context,
    version: bool = typer.Option(
        None,
        "--version",
        "-v",
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


if __name__ == "__main__":  # pragma: no cover
    app()
