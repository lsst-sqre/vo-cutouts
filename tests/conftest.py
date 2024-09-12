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
from safir.testing.uws import MockUWSJobRunner

from vocutouts import main
from vocutouts.config import config, uws


@pytest_asyncio.fixture
async def app(arq_queue: MockArqQueue) -> AsyncIterator[FastAPI]:
    """Return a configured test application.

    Initialize the database before creating the app to ensure that data is
    dropped from a persistent database between test cases.
    """
    logger = structlog.get_logger("vocutouts")
    await uws.initialize_uws_database(logger, reset=True, use_alembic=True)
    async with LifespanManager(main.app):
        # Ensure that all the components use the same mock arq queue.
        # Otherwise, the web application will use the one created in its
        # lifespan context manager.
        uws.override_arq_queue(arq_queue)
        yield main.app


@pytest.fixture
def arq_queue() -> MockArqQueue:
    return MockArqQueue()


@pytest_asyncio.fixture
async def client(app: FastAPI) -> AsyncIterator[AsyncClient]:
    """Return an ``httpx.AsyncClient`` configured to talk to the test app."""
    async with AsyncClient(
        transport=ASGITransport(app=app),
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


@pytest_asyncio.fixture
async def runner(arq_queue: MockArqQueue) -> AsyncIterator[MockUWSJobRunner]:
    async with MockUWSJobRunner(config.uws_config, arq_queue) as runner:
        yield runner
