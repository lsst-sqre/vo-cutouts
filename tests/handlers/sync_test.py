"""Tests for sync cutout requests."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from dramatiq import Worker

from vocutouts.config import uws_broker

if TYPE_CHECKING:
    from typing import Dict, List

    from httpx import AsyncClient


@pytest.mark.asyncio
async def test_sync(client: AsyncClient) -> None:
    worker = Worker(uws_broker, worker_timeout=100)
    worker.start()

    try:
        # GET request.
        r = await client.get(
            "/cutout/sync",
            headers={"X-Auth-Request-User": "someone"},
            params={"ID": "sync-id", "Pos": "RANGE 0 360 -2 2"},
        )
        assert r.status_code == 303
        assert r.headers["Location"] == "https://example.com/cutout/sync-id"

        # POST request.
        r = await client.post(
            "/cutout/sync",
            headers={"X-Auth-Request-User": "someone"},
            data={"ID": "other-id", "Pos": "RANGE 0 360 -2 2"},
        )
        assert r.status_code == 303
        assert r.headers["Location"] == "https://example.com/cutout/other-id"

        # Mixed request.
        r = await client.post(
            "/cutout/sync",
            headers={"X-Auth-Request-User": "someone"},
            params={"id": "mixed-id"},
            data={"pos": "RANGE 0 360 -2 2"},
        )
        assert r.status_code == 303
        assert r.headers["Location"] == "https://example.com/cutout/mixed-id"
    finally:
        worker.stop()


@pytest.mark.asyncio
async def test_bad_parameters(client: AsyncClient) -> None:
    bad_params: List[Dict[str, str]] = [
        {},
        {"pos": "RANGE 0 360 -2 2"},
        {"id": "foo", "foo": "bar"},
        {"id": "foo", "pos": "RANGE 0 360"},
        {"id": "foo", "pos": "POLYHEDRON 10"},
        {"id": "foo", "pos": "CIRCLE 1 1"},
        {"id": "foo", "pos": "POLYGON 1 1"},
        {"id": "foo", "circle": "1 1 1", "pos": "RANGE 0 360 1"},
        {"id": "foo", "circle": "1"},
        {"id": "foo", "polygon": "1 2 3"},
        {"id": "foo", "circle": "1 1 1", "phase": "STOP"},
        {"id": "foo", "circle": "1 1 1", "PHASE": "STOP"},
    ]
    for params in bad_params:
        r = await client.get(
            "/cutout/sync",
            headers={"X-Auth-Request-User": "user"},
            params=params,
        )
        assert r.status_code == 422, f"Parameters {params}"
        assert r.text.startswith("UsageError")
        r = await client.post(
            "/cutout/sync",
            headers={"X-Auth-Request-User": "user"},
            data=params,
        )
        assert r.status_code == 422, f"Parameters {params}"
        assert r.text.startswith("UsageError")
