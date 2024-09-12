"""Administrative command-line interface."""

from __future__ import annotations

import subprocess
from pathlib import Path

import click
import structlog
from safir.asyncio import run_with_asyncio
from safir.click import display_help

from .config import uws

__all__ = [
    "help",
    "init",
    "main",
]


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(message="%(version)s")
def main() -> None:
    """Administrative command-line interface for vo-cutouts."""


@main.command()
@click.argument("topic", default=None, required=False, nargs=1)
@click.pass_context
def help(ctx: click.Context, topic: str | None) -> None:
    """Show help for any command."""
    display_help(main, ctx, topic)


@main.command()
@click.option(
    "--alembic/--no-alembic",
    default=True,
    help="Mark the database with the current Alembic version.",
)
@click.option(
    "--alembic-config-path",
    envvar="CUTOUT_ALEMBIC_CONFIG_PATH",
    type=click.Path(path_type=Path),
    default=Path("/app/alembic.ini"),
    help="Alembic configuration file.",
)
@click.option(
    "--reset", is_flag=True, help="Delete all existing database data."
)
@run_with_asyncio
async def init(
    *, alembic: bool, alembic_config_path: Path, reset: bool
) -> None:
    """Initialize the database storage."""
    logger = structlog.get_logger("vocutouts")
    await uws.initialize_uws_database(
        logger,
        reset=reset,
        use_alembic=alembic,
        alembic_config_path=alembic_config_path,
    )


@main.command()
@click.option(
    "--alembic-config-path",
    envvar="CUTOUT_ALEMBIC_CONFIG_PATH",
    type=click.Path(path_type=Path),
    default=Path("/app/alembic.ini"),
    help="Alembic configuration file.",
)
def update_schema(*, alembic_config_path: Path) -> None:
    """Update the schema."""
    subprocess.run(
        ["alembic", "upgrade", "head"],
        check=True,
        cwd=str(alembic_config_path.parent),
    )


@main.command()
@click.option(
    "--alembic-config-path",
    envvar="CUTOUT_ALEMBIC_CONFIG_PATH",
    type=click.Path(path_type=Path),
    default=Path("/app/alembic.ini"),
    help="Alembic configuration file.",
)
@run_with_asyncio
async def validate_schema(*, alembic_config_path: Path) -> None:
    """Validate that the database schema is current."""
    logger = structlog.get_logger("vocutouts")
    if not uws.is_schema_current(logger, alembic_config_path):
        raise click.ClickException("Database schema is not current")
