"""pytest fixtures for UWS testing."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from datetime import timedelta

import pytest
import pytest_asyncio
import respx
import structlog
from asgi_lifespan import LifespanManager
from fastapi import APIRouter, FastAPI
from httpx import ASGITransport, AsyncClient
from safir.arq import MockArqQueue
from safir.middleware.ivoa import CaseInsensitiveQueryMiddleware
from safir.middleware.x_forwarded import XForwardedMiddleware
from safir.slack.webhook import SlackRouteErrorHandler
from safir.testing.gcs import MockStorageClient, patch_google_storage
from safir.testing.slack import MockSlackWebhook, mock_slack_webhook
from structlog.stdlib import BoundLogger

from vocutouts.uws.app import UWSApplication
from vocutouts.uws.config import UWSConfig
from vocutouts.uws.dependencies import UWSFactory, uws_dependency

from ..support.uws import MockJobRunner, build_uws_config


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
    uws = UWSApplication(uws_config)
    await uws.initialize_uws_database(logger, reset=True)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        await uws_dependency.initialize(uws_config)
        uws_dependency.override_arq_queue(arq_queue)
        yield
        await uws_dependency.aclose()

    app = FastAPI(lifespan=lifespan)
    app.add_middleware(CaseInsensitiveQueryMiddleware)
    app.add_middleware(XForwardedMiddleware)
    router = APIRouter(route_class=SlackRouteErrorHandler)
    uws.install_handlers(router)
    app.include_router(router, prefix="/test")
    uws.install_error_handlers(app)

    async with LifespanManager(app):
        yield app


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


@pytest.fixture(autouse=True)
def mock_slack(
    uws_config: UWSConfig, respx_mock: respx.Router
) -> MockSlackWebhook:
    assert uws_config.slack_webhook
    return mock_slack_webhook(
        uws_config.slack_webhook.get_secret_value(), respx_mock
    )


@pytest.fixture
def runner(uws_factory: UWSFactory, arq_queue: MockArqQueue) -> MockJobRunner:
    return MockJobRunner(uws_factory, arq_queue)


@pytest.fixture
def uws_config() -> UWSConfig:
    return build_uws_config()


@pytest_asyncio.fixture
async def uws_factory(
    app: FastAPI, logger: BoundLogger
) -> AsyncIterator[UWSFactory]:
    async for factory in uws_dependency(logger):
        yield factory
