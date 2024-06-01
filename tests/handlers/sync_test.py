"""Tests for sync cutout requests."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest
from httpx import AsyncClient
from safir.arq import MockArqQueue

from vocutouts.uws.dependencies import UWSFactory
from vocutouts.uws.models import UWSJobResult


@pytest.mark.asyncio
async def test_sync(
    client: AsyncClient, arq_queue: MockArqQueue, uws_factory: UWSFactory
) -> None:
    job_service = uws_factory.create_job_service()
    job_storage = uws_factory.create_job_store()

    async def set_result(job_id: str) -> None:
        await asyncio.sleep(0.1)
        job = await job_service.get("someone", job_id)
        assert job.message_id
        await arq_queue.set_in_progress(job.message_id)
        await job_storage.mark_executing(job_id, datetime.now(tz=UTC))
        result = [
            UWSJobResult(
                result_id="cutout",
                url="s3://some-bucket/some/path",
                mime_type="application/fits",
            )
        ]
        job = await job_service.get("someone", job_id)
        assert job.message_id
        await arq_queue.set_complete(job.message_id, result=result)
        job_result = await arq_queue.get_job_result(job.message_id)
        await job_storage.mark_completed(job_id, job_result)

    # GET request.
    _, r = await asyncio.gather(
        set_result("1"),
        client.get(
            "/api/cutout/sync",
            headers={"X-Auth-Request-User": "someone"},
            params={"ID": "1:2:band:id", "Pos": "CIRCLE 0 -2 2"},
        ),
    )
    assert r.status_code == 303
    assert r.headers["Location"] == "https://example.com/some/path"

    # POST request.
    _, r = await asyncio.gather(
        set_result("2"),
        client.post(
            "/api/cutout/sync",
            headers={"X-Auth-Request-User": "someone"},
            data={"ID": "3:4:band:id", "Pos": "CIRCLE 0 -2 2"},
        ),
    )
    assert r.status_code == 303
    assert r.headers["Location"] == "https://example.com/some/path"


@pytest.mark.asyncio
async def test_bad_parameters(client: AsyncClient) -> None:
    bad_params: list[dict[str, str]] = [
        {},
        {"pos": "RANGE 0 360 -2 2"},
        {"id": "5:6:a:b", "foo": "bar"},
        {"id": "5:6:a:b", "pos": "RANGE 0 360"},
        {"id": "5:6:a:b", "pos": "POLYHEDRON 10"},
        {"id": "5:6:a:b", "pos": "CIRCLE 1 1"},
        {"id": "5:6:a:b", "pos": "POLYGON 1 1"},
        {"id": "5:6:a:b", "circle": "1 1 1", "pos": "RANGE 0 360 1"},
        {"id": "5:6:a:b", "circle": "1"},
        {"id": "5:6:a:b", "polygon": "1 2 3"},
        {"id": "5:6:a:b", "circle": "1 1 1", "phase": "STOP"},
        {"id": "5:6:a:b", "circle": "1 1 1", "PHASE": "STOP"},
    ]
    for params in bad_params:
        r = await client.get(
            "/api/cutout/sync",
            headers={"X-Auth-Request-User": "user"},
            params=params,
        )
        assert r.status_code == 422, f"Parameters {params}"
        assert r.text.startswith("UsageError")
        r = await client.post(
            "/api/cutout/sync",
            headers={"X-Auth-Request-User": "user"},
            data=params,
        )
        assert r.status_code == 422, f"Parameters {params}"
        assert r.text.startswith("UsageError")
