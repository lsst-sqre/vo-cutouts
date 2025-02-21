"""The main application factory for the vo-cutouts service.

Notes
-----
Be aware that, following the normal pattern for FastAPI services, the app is
constructed when this module is loaded and is not deferred until a function is
called.
"""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from importlib.metadata import metadata, version

import structlog
from fastapi import FastAPI
from safir.dependencies.http_client import http_client_dependency
from safir.logging import Profile, configure_logging, configure_uvicorn_logging
from safir.middleware.x_forwarded import XForwardedMiddleware
from safir.models import ErrorModel
from safir.slack.webhook import SlackRouteErrorHandler

from .config import config, uws
from .handlers import external, internal

__all__ = ["app"]


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncGenerator[None]:
    """Set up and tear down the application."""
    await uws.initialize_fastapi()
    yield
    await uws.shutdown_fastapi()
    await http_client_dependency.aclose()


configure_logging(
    name="vocutouts", profile=config.profile, log_level=config.log_level
)
if config.profile == Profile.production:
    configure_uvicorn_logging(config.log_level)

app = FastAPI(
    title="vo-cutouts",
    description=metadata("vo-cutouts")["Summary"],
    version=version("vo-cutouts"),
    openapi_url=f"{config.path_prefix}/openapi.json",
    docs_url=f"{config.path_prefix}/docs",
    redoc_url=f"{config.path_prefix}/redoc",
    lifespan=_lifespan,
)
"""The main FastAPI application for vo-cutouts."""

# Attach the routers.
app.include_router(internal.router)
uws.install_handlers(external.router)
app.include_router(
    external.router,
    prefix=config.path_prefix,
    responses={
        401: {"description": "Unauthenticated"},
        403: {"description": "Permission denied", "model": ErrorModel},
    },
)

# Install middleware.
app.add_middleware(XForwardedMiddleware)
uws.install_middleware(app)

# Install error handlers.
uws.install_error_handlers(app)

# Configure Slack alerts.
if config.slack_webhook:
    logger = structlog.get_logger("vocutouts")
    SlackRouteErrorHandler.initialize(
        config.slack_webhook.get_secret_value(), "vo-cutouts", logger
    )
    logger.debug("Initialized Slack webhook")
