"""Test fixtures for vo-cutouts tests."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
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
from safir.testing.gcs import MockStorageClient, patch_google_storage

from vocutouts import main
from vocutouts.config import config
from vocutouts.policy import ImageCutoutPolicy
from vocutouts.uws.dependencies import UWSFactory, uws_dependency
from vocutouts.uws.schema import Base

from .support.uws import MockJobRunner


@pytest_asyncio.fixture
async def app(arq_queue: MockArqQueue) -> AsyncIterator[FastAPI]:
    """Return a configured test application.

    Initialize the database before creating the app to ensure that data is
    dropped from a persistent database between test cases.
    """
    logger = structlog.get_logger("vocutouts")
    engine = create_database_engine(
        config.database_url, config.database_password
    )
    await initialize_database(engine, logger, schema=Base.metadata, reset=True)
    await engine.dispose()
    async with LifespanManager(main.app):
        # Ensure that all the components use the same mock arq queue.
        # Otherwise, the web application will use the one created in its
        # lifespan context manager.
        uws_dependency.override_policy(ImageCutoutPolicy(arq_queue, logger))
        yield main.app


@pytest.fixture
def arq_queue() -> MockArqQueue:
    return MockArqQueue()


@pytest_asyncio.fixture
async def client(app: FastAPI) -> AsyncIterator[AsyncClient]:
    """Return an ``httpx.AsyncClient`` configured to talk to the test app."""
    async with AsyncClient(
        transport=ASGITransport(app=app),  # type: ignore[arg-type]
        base_url="https://example.com/",
        headers={"X-Auth-Request-Token": "sometoken"},
    ) as client:
        yield client


@pytest.fixture(autouse=True)
def mock_google_storage() -> Iterator[MockStorageClient]:
    yield from patch_google_storage(
        expected_expiration=timedelta(minutes=15), bucket_name="some-bucket"
    )


@pytest.fixture
def runner(uws_factory: UWSFactory, arq_queue: MockArqQueue) -> MockJobRunner:
    return MockJobRunner(uws_factory, arq_queue)


@pytest_asyncio.fixture
async def uws_factory(app: FastAPI) -> AsyncIterator[UWSFactory]:
    """Return a UWS component factory.

    Depends on the ``app`` fixture to ensure that the database layer has
    already been initialized.
    """
    logger = structlog.get_logger("vocutouts")
    async for session in db_session_dependency():
        yield await uws_dependency(session, logger)
