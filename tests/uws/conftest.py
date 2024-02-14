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

from collections.abc import AsyncIterator, Iterator
from datetime import timedelta

import pytest
import pytest_asyncio
import structlog
from asgi_lifespan import LifespanManager
from dramatiq import Broker
from fastapi import FastAPI
from httpx import AsyncClient
from safir.database import create_database_engine, initialize_database
from safir.dependencies.db_session import db_session_dependency
from safir.dependencies.http_client import http_client_dependency
from safir.middleware.ivoa import CaseInsensitiveQueryMiddleware
from safir.middleware.x_forwarded import XForwardedMiddleware
from safir.testing.gcs import MockStorageClient, patch_google_storage
from sqlalchemy.ext.asyncio import async_scoped_session
from structlog.stdlib import BoundLogger

from vocutouts.uws.config import UWSConfig
from vocutouts.uws.dependencies import UWSFactory, uws_dependency
from vocutouts.uws.errors import install_error_handlers
from vocutouts.uws.handlers import uws_router
from vocutouts.uws.schema import Base

from ..support.uws import (
    TrivialPolicy,
    WorkerSession,
    build_uws_config,
    trivial_job,
    uws_broker,
)


@pytest_asyncio.fixture
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
    engine = create_database_engine(
        uws_config.database_url, uws_config.database_password
    )
    await initialize_database(engine, logger, schema=Base.metadata, reset=True)
    await engine.dispose()
    uws_app = FastAPI()
    uws_app.include_router(uws_router, prefix="/jobs")
    uws_app.add_middleware(CaseInsensitiveQueryMiddleware)
    uws_app.add_middleware(XForwardedMiddleware)
    install_error_handlers(uws_app)
    uws_broker.add_middleware(WorkerSession(uws_config))

    @uws_app.on_event("startup")
    async def startup_event() -> None:
        await uws_dependency.initialize(
            config=uws_config,
            policy=TrivialPolicy(trivial_job),
            logger=logger,
        )

    @uws_app.on_event("shutdown")
    async def shutdown_event() -> None:
        await http_client_dependency.aclose()
        await uws_dependency.aclose()

    async with LifespanManager(uws_app):
        yield uws_app


@pytest_asyncio.fixture
async def client(app: FastAPI) -> AsyncIterator[AsyncClient]:
    """Return an ``httpx.AsyncClient`` configured to talk to the test app."""
    async with AsyncClient(
        app=app,
        base_url="https://example.com/",
        # Mock the Gafaelfawr delegated token header.
        headers={"X-Auth-Request-Token": "sometoken"},
    ) as client:
        yield client


@pytest.fixture
def logger() -> BoundLogger:
    return structlog.get_logger("uws")


@pytest.fixture(autouse=True)
def mock_google_storage() -> Iterator[MockStorageClient]:
    yield from patch_google_storage(
        expected_expiration=timedelta(minutes=15), bucket_name="some-bucket"
    )


@pytest_asyncio.fixture
async def session(app: FastAPI) -> AsyncIterator[async_scoped_session]:
    """Return a database session with no transaction open.

    Depends on the ``app`` fixture to ensure that the database layer has
    already been initialized.
    """
    async for session in db_session_dependency():
        yield session


@pytest.fixture
def stub_broker() -> Broker:
    uws_broker.emit_after("process_boot")
    uws_broker.flush_all()
    return uws_broker


@pytest.fixture
def uws_config() -> UWSConfig:
    return build_uws_config()


@pytest_asyncio.fixture
async def uws_factory(
    session: async_scoped_session, logger: BoundLogger
) -> UWSFactory:
    return await uws_dependency(session, logger)
