"""The main application factory for the vo-cutouts service.

Notes
-----
Be aware that, following the normal pattern for FastAPI services, the app is
constructed when this module is loaded and is not deferred until a function is
called.
"""

from importlib.metadata import metadata

import structlog
from fastapi import FastAPI
from safir.dependencies.http_client import http_client_dependency
from safir.logging import configure_logging
from safir.middleware.x_forwarded import XForwardedMiddleware

from .actors import cutout
from .config import config
from .handlers.external import external_router
from .handlers.internal import internal_router
from .policy import ImageCutoutPolicy
from .uws.dependencies import uws_dependency
from .uws.errors import install_error_handlers
from .uws.middleware import CaseInsensitiveQueryMiddleware

__all__ = ["app", "config"]


configure_logging(
    profile=config.profile,
    log_level=config.log_level,
    name=config.logger_name,
)

app = FastAPI(
    title="vo-cutouts",
    description=metadata("vo-cutouts").get("Summary", ""),
    version=metadata("vo-cutouts").get("Version", "0.0.0"),
    openapi_url=f"/{config.name}/openapi.json",
    docs_url=f"/{config.name}/docs",
    redoc_url=f"/{config.name}/redoc",
)
"""The main FastAPI application for vo-cutouts."""

# Attach the routers.
app.include_router(internal_router)
app.include_router(
    external_router,
    prefix=f"/{config.name}",
    responses={401: {"description": "Unauthenticated"}},
)


@app.on_event("startup")
async def startup_event() -> None:
    app.add_middleware(XForwardedMiddleware)
    app.add_middleware(CaseInsensitiveQueryMiddleware)
    logger = structlog.get_logger(config.logger_name)
    install_error_handlers(app)
    await uws_dependency.initialize(
        config=config.uws_config(),
        policy=ImageCutoutPolicy(cutout, logger),
        logger=logger,
    )


@app.on_event("shutdown")
async def shutdown_event() -> None:
    await http_client_dependency.aclose()
