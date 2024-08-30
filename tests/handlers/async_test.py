"""Tests for async job creation."""

from __future__ import annotations

import asyncio
import re

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from safir.testing.slack import MockSlackWebhook
from safir.testing.uws import MockUWSJobRunner
from safir.uws import UWSJobResult

from vocutouts.models.domain.cutout import WorkerCutout

PENDING_JOB = """
<uws:job
    version="1.1"
    xsi:schemaLocation="http://www.ivoa.net/xml/UWS/v1.0 UWS.xsd"
    xmlns:xml="http://www.w3.org/XML/1998/namespace"
    xmlns:uws="http://www.ivoa.net/xml/UWS/v1.0"
    xmlns:xlink="http://www.w3.org/1999/xlink"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <uws:jobId>1</uws:jobId>
  <uws:ownerId>someone</uws:ownerId>
  <uws:phase>PENDING</uws:phase>
  <uws:creationTime>[DATE]</uws:creationTime>
  <uws:executionDuration>600</uws:executionDuration>
  <uws:destruction>[DATE]</uws:destruction>
  <uws:parameters>
    <uws:parameter id="id">1:2:band:value</uws:parameter>
    <uws:parameter id="pos">CIRCLE 0 1 2</uws:parameter>
  </uws:parameters>
</uws:job>
"""

COMPLETED_JOB = """
<uws:job
    version="1.1"
    xsi:schemaLocation="http://www.ivoa.net/xml/UWS/v1.0 UWS.xsd"
    xmlns:xml="http://www.w3.org/XML/1998/namespace"
    xmlns:uws="http://www.ivoa.net/xml/UWS/v1.0"
    xmlns:xlink="http://www.w3.org/1999/xlink"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <uws:jobId>2</uws:jobId>
  <uws:runId>some-run-id</uws:runId>
  <uws:ownerId>someone</uws:ownerId>
  <uws:phase>COMPLETED</uws:phase>
  <uws:creationTime>[DATE]</uws:creationTime>
  <uws:startTime>[DATE]</uws:startTime>
  <uws:endTime>[DATE]</uws:endTime>
  <uws:executionDuration>600</uws:executionDuration>
  <uws:destruction>[DATE]</uws:destruction>
  <uws:parameters>
    <uws:parameter id="id">1:2:band:value</uws:parameter>
    <uws:parameter id="pos">CIRCLE 0.5 0.8 2</uws:parameter>
  </uws:parameters>
  <uws:results>
    <uws:result id="cutout" xlink:href="https://example.com/some/path"\
 mime-type="application/fits"/>
  </uws:results>
</uws:job>
"""


@pytest.mark.asyncio
async def test_create_job(
    client: AsyncClient, runner: MockUWSJobRunner
) -> None:
    r = await client.post(
        "/api/cutout/jobs",
        headers={"X-Auth-Request-User": "someone"},
        data={"ID": "1:2:band:value", "Pos": "CIRCLE 0 1 2"},
    )
    assert r.status_code == 303
    assert r.headers["Location"] == "https://example.com/api/cutout/jobs/1"
    r = await client.get(
        "/api/cutout/jobs/1", headers={"X-Auth-Request-User": "someone"}
    )
    assert r.status_code == 200
    result = re.sub(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ", "[DATE]", r.text)
    assert result == PENDING_JOB.strip()

    # Try again but immediately queuing the job to run and mark the job as
    # complete in parallel.
    r = await client.post(
        "/api/cutout/jobs",
        headers={"X-Auth-Request-User": "someone"},
        data={
            "ID": "1:2:band:value",
            "pos": "CIRCLE 0.5 0.8 2",
            "runid": "some-run-id",
        },
        params={"phase": "RUN"},
    )
    assert r.status_code == 303
    assert r.headers["Location"] == "https://example.com/api/cutout/jobs/2"

    async def run_job() -> None:
        arq_job = await runner.get_job_metadata("someone", "2")
        assert isinstance(arq_job.args[0], dict)
        assert WorkerCutout.model_validate(arq_job.args[0])
        await runner.mark_in_progress("someone", "2", delay=0.2)
        results = [
            UWSJobResult(
                result_id="cutout",
                url="s3://some-bucket/some/path",
                mime_type="application/fits",
            )
        ]
        await runner.mark_complete("someone", "2", results, delay=0.2)

    _, r = await asyncio.gather(
        run_job(),
        client.get(
            "/api/cutout/jobs/2",
            headers={"X-Auth-Request-User": "someone"},
            params={"wait": 2, "phase": "QUEUED"},
        ),
    )
    assert r.status_code == 200
    assert "EXECUTING" in r.text
    r = await client.get(
        "/api/cutout/jobs/2",
        headers={"X-Auth-Request-User": "someone"},
        params={"wait": 10, "phase": "EXECUTING"},
    )
    assert r.status_code == 200
    result = re.sub(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ", "[DATE]", r.text)
    assert result == COMPLETED_JOB.strip()


@pytest.mark.asyncio
async def test_redirect(app: FastAPI) -> None:
    """Test the scheme in the redirect after creating a job.

    When running in a Kubernetes cluster behind an ingress that terminates
    TLS, the request as seen by the application will be ``http``, but we want
    the redirect to honor ``X-Forwarded-Proto`` and thus use ``https``.  Also
    test that the correct hostname is used if it is different.
    """
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://foo.com/",
        headers={"X-Auth-Request-Token": "sometoken"},
    ) as client:
        r = await client.post(
            "/api/cutout/jobs",
            headers={
                "Host": "example.com",
                "X-Forwarded-For": "10.10.10.10",
                "X-Forwarded-Host": "example.com",
                "X-Forwarded-Proto": "https",
                "X-Auth-Request-User": "someone",
            },
            data={"ID": "1:2:band:value", "Pos": "CIRCLE 0 1 2"},
        )
    assert r.status_code == 303
    assert r.headers["Location"] == "https://example.com/api/cutout/jobs/1"


@pytest.mark.asyncio
async def test_bad_parameters(
    client: AsyncClient, mock_slack: MockSlackWebhook
) -> None:
    bad_params: list[dict[str, str]] = [
        {},
        {"pos": "RANGE 0 360 -2 2"},
        {"id": "foo", "foo": "bar"},
        {"id": "foo", "pos": "RANGE 0 360"},
        {"id": "foo", "pos": "POLYHEDRON 10"},
        {"id": "foo", "pos": "CIRCLE 1 1"},
        {"id": "foo", "pos": "POLYGON 1 1"},
        {"id": "foo", "circle": "1"},
        {"id": "foo", "polygon": "1 2 3"},
        {"id": "foo", "circle": "1 1 1", "pos": "RANGE 0 360 1"},
        {"ID": "some-id", "pos": "RANGE 1 1 2 2", "phase": "RUN"},
    ]
    for params in bad_params:
        r = await client.post(
            "/api/cutout/jobs",
            headers={"X-Auth-Request-User": "user"},
            data=params,
        )
        assert r.status_code == 422, f"Parameters {params}"
        assert r.text.startswith("UsageError")

    # Test requesting two stencils.
    r = await client.post(
        "/api/cutout/jobs",
        headers={"X-Auth-Request-User": "user"},
        data={"id": "foo", "circle": "1 1 1", "pos": "CIRCLE 2 2 2"},
    )
    assert r.status_code == 422, f"Parameters {params}"
    assert r.text.startswith("MultiValuedParamNotSupported")

    # None of these requests should have been reported to Slack.
    assert mock_slack.messages == []
