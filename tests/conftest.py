"""Test fixtures for vo-cutouts tests."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from datetime import timedelta
from typing import Annotated

import pytest
import pytest_asyncio
import respx
import structlog
from asgi_lifespan import LifespanManager
from fastapi import APIRouter, Depends, FastAPI
from httpx import ASGITransport, AsyncClient
from safir.arq import MockArqQueue
from safir.testing.gcs import MockStorageClient, patch_google_storage
from safir.testing.slack import MockSlackWebhook, mock_slack_webhook
from safir.testing.uws import MockUWSJobRunner
from safir.uws import UWSJobParameter

from vocutouts import main
from vocutouts.config import config, uws
from vocutouts.dependencies import get_params_dependency


@pytest.fixture
def get_params_router() -> APIRouter:
    """Return a router that echoes the parameters passed to it."""
    router = APIRouter()

    @router.get("/params")
    async def get_params(
        params: Annotated[
            list[UWSJobParameter], Depends(get_params_dependency)
        ],
    ) -> dict[str, list[dict[str, str]]]:
        return {
            "params": [
                {"id": p.parameter_id, "value": p.value} for p in params
            ]
        }

    return router


@pytest_asyncio.fixture
async def app(
    arq_queue: MockArqQueue, get_params_router: APIRouter
) -> AsyncIterator[FastAPI]:
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
        main.app.include_router(get_params_router)
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
