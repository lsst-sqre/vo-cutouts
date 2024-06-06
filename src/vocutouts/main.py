"""The main application factory for the vo-cutouts service.

Notes
-----
Be aware that, following the normal pattern for FastAPI services, the app is
constructed when this module is loaded and is not deferred until a function is
called.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from importlib.metadata import metadata, version

import structlog
from fastapi import FastAPI
from safir.arq import ArqMode, ArqQueue, MockArqQueue, RedisArqQueue
from safir.logging import Profile, configure_logging, configure_uvicorn_logging
from safir.middleware.ivoa import CaseInsensitiveQueryMiddleware
from safir.middleware.x_forwarded import XForwardedMiddleware

from .config import config
from .handlers.external import external_router
from .handlers.internal import internal_router
from .policy import ImageCutoutPolicy
from .uws.dependencies import uws_dependency
from .uws.errors import install_error_handlers

__all__ = ["app", "lifespan"]


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Set up and tear down the application."""
    if config.arq_mode == ArqMode.production:
        settings = config.arq_redis_settings
        arq: ArqQueue = await RedisArqQueue.initialize(settings)
    else:
        arq = MockArqQueue()
    logger = structlog.get_logger("vocutouts")
    policy = ImageCutoutPolicy(arq, logger)
    await uws_dependency.initialize(config.uws_config, policy)

    yield

    await uws_dependency.aclose()


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
    lifespan=lifespan,
)
"""The main FastAPI application for vo-cutouts."""

# Attach the routers.
app.include_router(internal_router)
app.include_router(
    external_router,
    prefix=config.path_prefix,
    responses={401: {"description": "Unauthenticated"}},
)

# Install middleware.
app.add_middleware(XForwardedMiddleware)
app.add_middleware(CaseInsensitiveQueryMiddleware)

# Install error handlers.
install_error_handlers(app)
