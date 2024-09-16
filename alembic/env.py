"""Alembic migration environment."""

from safir.database import run_migrations_offline, run_migrations_online
from safir.logging import configure_alembic_logging, configure_logging
from safir.uws import UWSSchemaBase

from alembic import context
from vocutouts.config import config

# Configure structlog.
configure_logging(name="vo-cutouts", log_level=config.log_level)
configure_alembic_logging()

# Run the migrations.
if context.is_offline_mode():
    run_migrations_offline(UWSSchemaBase.metadata, config.database_url)
else:
    run_migrations_online(
        UWSSchemaBase.metadata,
        config.database_url,
        config.database_password,
    )
