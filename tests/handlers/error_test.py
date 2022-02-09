"""Test that errors produce a proper backtrace."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from dramatiq import Worker
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.ext.asyncio import create_async_engine

from vocutouts.broker import broker
from vocutouts.config import config
from vocutouts.uws.database import _build_database_url
from vocutouts.uws.schema import drop_schema

if TYPE_CHECKING:
    from httpx import AsyncClient


@pytest.mark.asyncio
async def test_uncaught_error(client: AsyncClient) -> None:
    worker = Worker(broker, worker_timeout=100)
    worker.start()

    # Drop the schema, which will ensure that all database errors will throw a
    # SQLAlchemy exception.  Previously this would result in a 500 error with
    # no meaningful information and no exception traceback due a bug in
    # swallowing errors in subapps.
    url = _build_database_url(config.uws_config(), is_async=True)
    engine = create_async_engine(url)
    await drop_schema(engine)

    # Now try to start a job, which should throw a meaningful exception.
    with pytest.raises(ProgrammingError):
        await client.get(
            "/cutout/sync",
            headers={"X-Auth-Request-User": "someone"},
            params={"ID": "1:2:band:id", "Pos": "CIRCLE 0 -2 2"},
        )
