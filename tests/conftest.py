"""Test fixtures for vo-cutouts tests."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING
from unittest.mock import Mock, patch

import dramatiq
import pytest
import structlog
from asgi_lifespan import LifespanManager
from dramatiq.middleware import CurrentMessage
from httpx import AsyncClient
from lsst.daf.butler import Butler, ButlerURI

from vocutouts import main
from vocutouts.actors import job_started
from vocutouts.broker import broker
from vocutouts.config import config
from vocutouts.policy import ImageCutoutPolicy
from vocutouts.uws.database import initialize_database
from vocutouts.uws.dependencies import uws_dependency
from vocutouts.uws.utils import isodatetime

if TYPE_CHECKING:
    from typing import Any, AsyncIterator, Dict, Iterator, List

    from fastapi import FastAPI


@dramatiq.actor(queue_name="cutout", store_results=True)
def cutout_test(
    job_id: str,
    data_id: Dict[str, str],
    ra_min: float,
    ra_max: float,
    dec_min: float,
    dec_max: float,
) -> List[Dict[str, Any]]:
    message = CurrentMessage.get_current_message()
    now = isodatetime(datetime.now(tz=timezone.utc))
    job_started.send(job_id, message.message_id, now)
    return [
        {
            "result_id": "cutout",
            "collection": "output/collection",
            "data_id": {"visit": 903332, "detector": 20, "instrument": "HSC"},
            "datatype": "calexp_cutouts",
            "mime_type": "application/fits",
        }
    ]


@pytest.fixture
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
        uws_dependency.override_policy(ImageCutoutPolicy(cutout_test))
        yield main.app


@pytest.fixture
async def client(app: FastAPI) -> AsyncIterator[AsyncClient]:
    """Return an ``httpx.AsyncClient`` configured to talk to the test app."""
    async with AsyncClient(app=app, base_url="https://example.com/") as client:
        yield client


def _mock_butler_getURI(
    datatype: str, *, dataId: Dict[str, str], collections: List[str]
) -> ButlerURI:
    assert datatype == "calexp_cutouts"
    assert dataId == {"visit": 903332, "detector": 20, "instrument": "HSC"}
    assert collections == ["output/collection"]
    mock = Mock(spec=ButlerURI)
    mock.geturl.return_value = "https://example.com/cutout-result"
    return mock


@pytest.fixture(autouse=True)
def mock_butler() -> Iterator[None]:
    with patch("vocutouts.uws.butler.Butler") as mock:
        mock.return_value = Mock(spec=Butler)
        mock.return_value.getURI.side_effect = _mock_butler_getURI
        yield
