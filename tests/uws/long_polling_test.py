"""Test for long polling when retrieving jobs."""

from __future__ import annotations

import time
from datetime import timedelta
from typing import Any

import dramatiq
import pytest
from dramatiq import Worker
from dramatiq.middleware import CurrentMessage
from httpx import AsyncClient
from safir.datetime import current_datetime, isodatetime
from structlog.stdlib import BoundLogger

from tests.support.uws import TrivialPolicy, job_started, uws_broker
from vocutouts.uws.config import UWSConfig
from vocutouts.uws.dependencies import UWSFactory, uws_dependency
from vocutouts.uws.models import JobParameter

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
    logger: BoundLogger,
    uws_config: UWSConfig,
    uws_factory: UWSFactory,
) -> None:
    job_service = uws_factory.create_job_service()
    job = await job_service.create(
        "user",
        params=[
            JobParameter(parameter_id="id", value="bar"),
        ],
    )

    # Poll for changes for two seconds.  Nothing will happen since there is no
    # worker.
    now = current_datetime()
    r = await client.get(
        "/jobs/1",
        headers={"X-Auth-Request-User": "user"},
        params={"WAIT": "2"},
    )
    assert (current_datetime() - now).total_seconds() >= 2
    assert r.status_code == 200
    assert r.text == PENDING_JOB.strip().format(
        "PENDING",
        isodatetime(job.creation_time),
        isodatetime(job.creation_time + timedelta(seconds=24 * 60 * 60)),
    )

    @dramatiq.actor(broker=uws_broker, queue_name="job", store_results=True)
    def wait_job(job_id: str) -> list[dict[str, Any]]:
        message = CurrentMessage.get_current_message()
        now = isodatetime(current_datetime())
        job_started.send(job_id, message.message_id, now)
        time.sleep(2)
        return [
            {
                "result_id": "cutout",
                "url": "s3://some-bucket/some/path",
                "mime_type": "application/fits",
            }
        ]

    # Start the job and worker.
    uws_dependency.override_policy(TrivialPolicy(wait_job))
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
    now = current_datetime()
    worker = Worker(uws_broker, worker_timeout=100)
    worker.start()

    # Now, wait again.  We should get a reply after a couple of seconds when
    # the job finishes.
    try:
        r = await client.get(
            "/jobs/1",
            headers={"X-Auth-Request-User": "user"},
            params={"WAIT": "2", "phase": "QUEUED"},
        )
        assert r.status_code == 200
        job = await job_service.get("user", "1")
        assert job.start_time
        assert r.text == EXECUTING_JOB.strip().format(
            isodatetime(job.creation_time),
            isodatetime(job.start_time),
            isodatetime(job.creation_time + timedelta(seconds=24 * 60 * 60)),
        )
        r = await client.get(
            "/jobs/1",
            headers={"X-Auth-Request-User": "user"},
            params={"WAIT": "2", "phase": "EXECUTING"},
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
    finally:
        worker.stop()
