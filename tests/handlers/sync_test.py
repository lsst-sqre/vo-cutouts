"""Tests for sync cutout requests."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from dramatiq import Worker

from vocutouts.broker import broker

if TYPE_CHECKING:
    from typing import Dict, List

    from httpx import AsyncClient


@pytest.mark.asyncio
async def test_sync(client: AsyncClient) -> None:
    worker = Worker(broker, worker_timeout=100)
    worker.start()

    try:
        # GET request.
        r = await client.get(
            "/cutout/sync",
            headers={"X-Auth-Request-User": "someone"},
            params={"ID": "a:sync:band:id", "Pos": "RANGE 0 360 -2 2"},
        )
        assert r.status_code == 303
        assert r.headers["Location"] == "https://example.com/cutout-result"

        # POST request.
        r = await client.post(
            "/cutout/sync",
            headers={"X-Auth-Request-User": "someone"},
            data={"ID": "a:other:band:id", "Pos": "RANGE 0 360 -2 2"},
        )
        assert r.status_code == 303
        assert r.headers["Location"] == "https://example.com/cutout-result"
    finally:
        worker.stop()


@pytest.mark.asyncio
async def test_bad_parameters(client: AsyncClient) -> None:
    bad_params: List[Dict[str, str]] = [
        {},
        {"pos": "RANGE 0 360 -2 2"},
        {"id": "a:a:foo", "foo": "bar"},
        {"id": "a:a:foo", "pos": "RANGE 0 360"},
        {"id": "a:a:foo", "pos": "POLYHEDRON 10"},
        {"id": "a:a:foo", "pos": "CIRCLE 1 1"},
        {"id": "a:a:foo", "pos": "POLYGON 1 1"},
        {"id": "a:a:foo", "circle": "1 1 1", "pos": "RANGE 0 360 1"},
        {"id": "a:a:foo", "circle": "1"},
        {"id": "a:a:foo", "polygon": "1 2 3"},
        {"id": "a:a:foo", "circle": "1 1 1", "phase": "STOP"},
        {"id": "a:a:foo", "circle": "1 1 1", "PHASE": "STOP"},
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
