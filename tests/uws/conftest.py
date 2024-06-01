"""pytest fixtures for UWS testing."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from datetime import timedelta

import pytest
import pytest_asyncio
import structlog
from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from safir.arq import MockArqQueue
from safir.database import create_database_engine, initialize_database
from safir.dependencies.db_session import db_session_dependency
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

from ..support.uws import TrivialPolicy, build_uws_config


@pytest_asyncio.fixture
async def app(
    arq_queue: MockArqQueue,
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

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        policy = TrivialPolicy(arq_queue, "trivial")
        await uws_dependency.initialize(uws_config, policy)
        yield
        await uws_dependency.aclose()

    uws_app = FastAPI(lifespan=lifespan)
    uws_app.include_router(uws_router, prefix="/jobs")
    uws_app.add_middleware(CaseInsensitiveQueryMiddleware)
    uws_app.add_middleware(XForwardedMiddleware)
    install_error_handlers(uws_app)

    async with LifespanManager(uws_app):
        yield uws_app


@pytest.fixture
def arq_queue() -> MockArqQueue:
    return MockArqQueue()


@pytest_asyncio.fixture
async def client(app: FastAPI) -> AsyncIterator[AsyncClient]:
    """Return an ``httpx.AsyncClient`` configured to talk to the test app."""
    transport = ASGITransport(app=app)  # type: ignore[arg-type]
    async with AsyncClient(
        transport=transport,
        base_url="https://example.com/",
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
def uws_config() -> UWSConfig:
    return build_uws_config()


@pytest_asyncio.fixture
async def uws_factory(
    session: async_scoped_session, logger: BoundLogger
) -> UWSFactory:
    return await uws_dependency(session, logger)
