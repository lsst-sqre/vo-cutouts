"""Tests for sync cutout requests."""

from __future__ import annotations

import asyncio

import pytest
from httpx import AsyncClient
from safir.testing.slack import MockSlackWebhook
from safir.testing.uws import MockUWSJobRunner
from safir.uws import UWSJobResult


@pytest.mark.asyncio
async def test_sync(client: AsyncClient, runner: MockUWSJobRunner) -> None:
    async def run_job(job_id: str) -> None:
        await runner.mark_in_progress("someone", job_id, delay=0.2)
        results = [
            UWSJobResult(
                result_id="cutout",
                url="s3://some-bucket/some/path",
                mime_type="application/fits",
            )
        ]
        await runner.mark_complete("someone", job_id, results)

    # GET request.
    _, r = await asyncio.gather(
        run_job("1"),
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
        run_job("2"),
        client.post(
            "/api/cutout/sync",
            headers={"X-Auth-Request-User": "someone"},
            data={"ID": "3:4:band:id", "Pos": "CIRCLE 0 -2 2"},
        ),
    )
    assert r.status_code == 303
    assert r.headers["Location"] == "https://example.com/some/path"


@pytest.mark.asyncio
async def test_bad_parameters(
    client: AsyncClient, mock_slack: MockSlackWebhook
) -> None:
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

    # None of these requests should have been reported to Slack.
    assert mock_slack.messages == []
