"""Test fixtures for vo-cutouts tests."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

import dramatiq
import pytest
import pytest_asyncio
import structlog
from asgi_lifespan import LifespanManager
from dramatiq.middleware import CurrentMessage
from httpx import AsyncClient

from vocutouts import main
from vocutouts.actors import job_started
from vocutouts.broker import broker
from vocutouts.config import config
from vocutouts.policy import ImageCutoutPolicy
from vocutouts.uws.database import initialize_database
from vocutouts.uws.dependencies import uws_dependency
from vocutouts.uws.utils import isodatetime

from .support.uws import mock_uws_google_storage

if TYPE_CHECKING:
    from typing import Any, AsyncIterator, Dict, Iterator, List

    from fastapi import FastAPI


@dramatiq.actor(queue_name="cutout", store_results=True)
def cutout_test(
    job_id: str,
    dataset_ids: List[str],
    stencils: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    message = CurrentMessage.get_current_message()
    now = isodatetime(datetime.now(tz=timezone.utc))
    job_started.send(job_id, message.message_id, now)
    assert len(dataset_ids) == 1
    return [
        {
            "result_id": "cutout",
            "url": "s3://some-bucket/some/path",
            "mime_type": "application/fits",
        }
    ]


@pytest_asyncio.fixture
async def app() -> AsyncIterator[FastAPI]:
    """Return a configured test application.

    Wraps the application in a lifespan manager so that startup and shutdown
    events are sent during test execution.  Initializes the database before
    creating the app to ensure that data is dropped from a persistent database
    between test cases.
    """
    logger = structlog.get_logger(config.logger_name)
    uws_config = config.uws_config()
    broker.flush_all()
    broker.emit_after("process_boot")
    await initialize_database(uws_config, logger, reset=True)
    async with LifespanManager(main.app):
        uws_dependency.override_policy(ImageCutoutPolicy(cutout_test, logger))
        yield main.app


@pytest_asyncio.fixture
async def client(app: FastAPI) -> AsyncIterator[AsyncClient]:
    """Return an ``httpx.AsyncClient`` configured to talk to the test app."""
    async with AsyncClient(app=app, base_url="https://example.com/") as client:
        yield client


@pytest.fixture(autouse=True)
def mock_google_storage() -> Iterator[None]:
    yield from mock_uws_google_storage()
