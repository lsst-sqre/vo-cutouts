"""Test fixtures for vo-cutouts tests."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from datetime import timedelta

import pytest
import pytest_asyncio
import respx
import structlog
from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from safir.arq import MockArqQueue
from safir.testing.gcs import MockStorageClient, patch_google_storage
from safir.testing.slack import MockSlackWebhook, mock_slack_webhook

from vocutouts import main
from vocutouts.config import config, uws
from vocutouts.uws.dependencies import UWSFactory, uws_dependency

from .support.uws import MockJobRunner


@pytest_asyncio.fixture
async def app(arq_queue: MockArqQueue) -> AsyncIterator[FastAPI]:
    """Return a configured test application.

    Initialize the database before creating the app to ensure that data is
    dropped from a persistent database between test cases.
    """
    logger = structlog.get_logger("vocutouts")
    await uws.initialize_uws_database(logger, reset=True)
    async with LifespanManager(main.app):
        # Ensure that all the components use the same mock arq queue.
        # Otherwise, the web application will use the one created in its
        # lifespan context manager.
        uws_dependency.override_arq_queue(arq_queue)
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


@pytest.fixture(autouse=True)
def mock_slack(respx_mock: respx.Router) -> MockSlackWebhook:
    assert config.slack_webhook
    webhook = config.slack_webhook.get_secret_value()
    return mock_slack_webhook(webhook, respx_mock)


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
    async for factory in uws_dependency(logger):
        yield factory
