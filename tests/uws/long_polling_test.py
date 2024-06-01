"""Test for long polling when retrieving jobs."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from httpx import AsyncClient
from safir.arq import MockArqQueue
from safir.datetime import current_datetime, isodatetime

from vocutouts.uws.dependencies import UWSFactory
from vocutouts.uws.models import UWSJobParameter, UWSJobResult

PENDING_JOB = """
<uws:job
    version="1.1"
    xsi:schemaLocation="http://www.ivoa.net/xml/UWS/v1.0 UWS.xsd"
    xmlns:xml="http://www.w3.org/XML/1998/namespace"
    xmlns:uws="http://www.ivoa.net/xml/UWS/v1.0"
    xmlns:xlink="http://www.w3.org/1999/xlink"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <uws:jobId>1</uws:jobId>
  <uws:ownerId>user</uws:ownerId>
  <uws:phase>{}</uws:phase>
  <uws:creationTime>{}</uws:creationTime>
  <uws:executionDuration>600</uws:executionDuration>
  <uws:destruction>{}</uws:destruction>
  <uws:parameters>
    <uws:parameter id="id">bar</uws:parameter>
  </uws:parameters>
</uws:job>
"""

EXECUTING_JOB = """
<uws:job
    version="1.1"
    xsi:schemaLocation="http://www.ivoa.net/xml/UWS/v1.0 UWS.xsd"
    xmlns:xml="http://www.w3.org/XML/1998/namespace"
    xmlns:uws="http://www.ivoa.net/xml/UWS/v1.0"
    xmlns:xlink="http://www.w3.org/1999/xlink"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <uws:jobId>1</uws:jobId>
  <uws:ownerId>user</uws:ownerId>
  <uws:phase>EXECUTING</uws:phase>
  <uws:creationTime>{}</uws:creationTime>
  <uws:startTime>{}</uws:startTime>
  <uws:executionDuration>600</uws:executionDuration>
  <uws:destruction>{}</uws:destruction>
  <uws:parameters>
    <uws:parameter id="id">bar</uws:parameter>
  </uws:parameters>
</uws:job>
"""

FINISHED_JOB = """
<uws:job
    version="1.1"
    xsi:schemaLocation="http://www.ivoa.net/xml/UWS/v1.0 UWS.xsd"
    xmlns:xml="http://www.w3.org/XML/1998/namespace"
    xmlns:uws="http://www.ivoa.net/xml/UWS/v1.0"
    xmlns:xlink="http://www.w3.org/1999/xlink"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <uws:jobId>1</uws:jobId>
  <uws:ownerId>user</uws:ownerId>
  <uws:phase>COMPLETED</uws:phase>
  <uws:creationTime>{}</uws:creationTime>
  <uws:startTime>{}</uws:startTime>
  <uws:endTime>{}</uws:endTime>
  <uws:executionDuration>600</uws:executionDuration>
  <uws:destruction>{}</uws:destruction>
  <uws:parameters>
    <uws:parameter id="id">bar</uws:parameter>
  </uws:parameters>
  <uws:results>
    <uws:result id="cutout" xlink:href="https://example.com/some/path"\
 mime-type="application/fits"/>
  </uws:results>
</uws:job>
"""


@pytest.mark.asyncio
async def test_poll(
    client: AsyncClient,
    arq_queue: MockArqQueue,
    uws_factory: UWSFactory,
) -> None:
    job_service = uws_factory.create_job_service()
    job_storage = uws_factory.create_job_store()
    job = await job_service.create(
        "user",
        params=[UWSJobParameter(parameter_id="id", value="bar")],
    )

    # Poll for changes for one second. Nothing will happen since nothing is
    # changing the mock arq queue.
    now = current_datetime()
    r = await client.get(
        "/jobs/1",
        headers={"X-Auth-Request-User": "user"},
        params={"WAIT": "1"},
    )
    assert (current_datetime() - now).total_seconds() >= 1
    assert r.status_code == 200
    assert r.text == PENDING_JOB.strip().format(
        "PENDING",
        isodatetime(job.creation_time),
        isodatetime(job.creation_time + timedelta(seconds=24 * 60 * 60)),
    )

    # Start the job and worker.
    r = await client.post(
        "/jobs/1/phase",
        headers={"X-Auth-Request-User": "user"},
        data={"PHASE": "RUN"},
        follow_redirects=True,
    )
    assert r.status_code == 200
    assert r.url == "https://example.com/jobs/1"
    assert r.text == PENDING_JOB.strip().format(
        "QUEUED",
        isodatetime(job.creation_time),
        isodatetime(job.creation_time + timedelta(seconds=24 * 60 * 60)),
    )

    async def set_in_progress() -> None:
        await asyncio.sleep(0.5)
        job = await job_service.get("user", "1")
        assert job.message_id
        await arq_queue.set_in_progress(job.message_id)
        await job_storage.mark_executing("1", datetime.now(tz=UTC))

    # Poll for a change from queued, which we should see after about a second.
    now = current_datetime()
    _, r = await asyncio.gather(
        set_in_progress(),
        client.get(
            "/jobs/1",
            headers={"X-Auth-Request-User": "user"},
            params={"WAIT": "2", "phase": "QUEUED"},
        ),
    )
    assert r.status_code == 200
    job = await job_service.get("user", "1")
    assert job.start_time
    assert r.text == EXECUTING_JOB.strip().format(
        isodatetime(job.creation_time),
        isodatetime(job.start_time),
        isodatetime(job.creation_time + timedelta(seconds=24 * 60 * 60)),
    )

    async def set_result() -> None:
        result = [
            UWSJobResult(
                result_id="cutout",
                url="s3://some-bucket/some/path",
                mime_type="application/fits",
            )
        ]
        await asyncio.sleep(1.5)
        job = await job_service.get("user", "1")
        assert job.message_id
        await arq_queue.set_complete(job.message_id, result=result)
        job_result = await arq_queue.get_job_result(job.message_id)
        await job_storage.mark_completed("1", job_result)

    # Now, wait again, in parallel with the job finishing. We should get a
    # reply after a couple of seconds when the job finishes.
    _, r = await asyncio.gather(
        set_result(),
        client.get(
            "/jobs/1",
            headers={"X-Auth-Request-User": "user"},
            params={"WAIT": "2", "phase": "EXECUTING"},
        ),
    )
    assert r.status_code == 200
    job = await job_service.get("user", "1")
    assert job.start_time
    assert job.end_time
    assert r.text == FINISHED_JOB.strip().format(
        isodatetime(job.creation_time),
        isodatetime(job.start_time),
        isodatetime(job.end_time),
        isodatetime(job.creation_time + timedelta(seconds=24 * 60 * 60)),
    )
    assert (current_datetime() - now).total_seconds() >= 2
