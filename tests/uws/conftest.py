"""pytest fixtures for UWS testing.

The long-term goal is for the UWS library to move into Safir, so there is some
attempt here to keep its tests independent.  The places where that is
currently violated are module naming (unavoidable), use of the image cutouts
app instead of a standalone app, use of the broker initialization from
vocutouts.config, and the naming of the environment variables used to generate
the UWSConfig.  These will all need to be fixed before this code can be moved
to Safir.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
import structlog
from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from httpx import AsyncClient
from safir.dependencies.http_client import http_client_dependency

from tests.support.uws import (
    TrivialPolicy,
    WorkerSession,
    build_uws_config,
    mock_uws_butler,
    trivial_job,
    uws_broker,
)
from vocutouts.uws.database import create_async_session, initialize_database
from vocutouts.uws.dependencies import UWSFactory, uws_dependency
from vocutouts.uws.errors import install_error_handlers
from vocutouts.uws.handlers import uws_router
from vocutouts.uws.middleware import CaseInsensitiveQueryMiddleware

if TYPE_CHECKING:
    from pathlib import Path
    from typing import AsyncIterator, Iterator

    from dramatiq import Broker
    from sqlalchemy.ext.asyncio import async_scoped_session
    from structlog.stdlib import BoundLogger

    from vocutouts.uws.config import UWSConfig


@pytest.fixture
async def app(
    stub_broker: Broker,
    uws_config: UWSConfig,
    logger: BoundLogger,
) -> AsyncIterator[FastAPI]:
    """Return a configured test application for UWS.

    This is a stand-alone test application independent of any real web
    application so that the UWS routes can be tested without reference to
    the pieces added by an application.
    """
    await initialize_database(uws_config, logger, reset=True)
    uws_app = FastAPI()
    uws_app.include_router(uws_router, prefix="/jobs")
    uws_broker.add_middleware(WorkerSession(uws_config))

    @uws_app.on_event("startup")
    async def startup_event() -> None:
        install_error_handlers(uws_app)
        uws_app.add_middleware(CaseInsensitiveQueryMiddleware)
        await uws_dependency.initialize(
            config=uws_config,
            policy=TrivialPolicy(trivial_job),
            logger=logger,
        )

    @uws_app.on_event("shutdown")
    async def shutdown_event() -> None:
        await http_client_dependency.aclose()

    async with LifespanManager(uws_app):
        yield uws_app


@pytest.fixture
async def client(app: FastAPI) -> AsyncIterator[AsyncClient]:
    """Return an ``httpx.AsyncClient`` configured to talk to the test app."""
    async with AsyncClient(app=app, base_url="https://example.com/") as client:
        yield client


@pytest.fixture
def logger() -> BoundLogger:
    return structlog.get_logger("uws")


@pytest.fixture(autouse=True)
def mock_butler() -> Iterator[None]:
    yield from mock_uws_butler()


@pytest.fixture
async def session(
    uws_config: UWSConfig, logger: BoundLogger
) -> async_scoped_session:
    return await create_async_session(uws_config, logger)


@pytest.fixture
def stub_broker() -> Broker:
    uws_broker.emit_after("process_boot")
    uws_broker.flush_all()
    return uws_broker


@pytest.fixture
def uws_config(tmp_path: Path) -> UWSConfig:
    return build_uws_config(tmp_path)


@pytest.fixture
async def uws_factory(app: FastAPI) -> AsyncIterator[UWSFactory]:
    async for factory in uws_dependency():
        yield factory
