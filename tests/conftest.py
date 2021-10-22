"""Test fixtures for vo-cutouts tests."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
import structlog
from asgi_lifespan import LifespanManager
from httpx import AsyncClient

from vocutouts import main
from vocutouts.config import config, uws_broker
from vocutouts.uws.database import initialize_database

if TYPE_CHECKING:
    from typing import AsyncIterator

    from fastapi import FastAPI


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
    uws_broker.flush_all()
    uws_broker.emit_after("process_boot")
    await initialize_database(uws_config, logger, reset=True)
    async with LifespanManager(main.app):
        yield main.app


@pytest.fixture
async def client(app: FastAPI) -> AsyncIterator[AsyncClient]:
    """Return an ``httpx.AsyncClient`` configured to talk to the test app."""
    async with AsyncClient(app=app, base_url="https://example.com/") as client:
        yield client
