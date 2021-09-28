"""Test for long polling when retrieving jobs."""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

import dramatiq
import pytest

from tests.support.uws import uws_broker
from vocutouts.uws.dependencies import uws_dependency
from vocutouts.uws.models import JobParameter, JobResult
from vocutouts.uws.tasks import uws_worker
from vocutouts.uws.utils import isodate

if TYPE_CHECKING:
    from typing import List

    from dramatiq import Worker
    from httpx import AsyncClient
    from structlog.stdlib import BoundLogger

    from vocutouts.uws.config import UWSConfig
    from vocutouts.uws.dependencies import UWSFactory


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
    <uws:result id="cutout" xlink:href="https://example.com/"/>
  </uws:results>
</uws:job>
"""


@pytest.mark.asyncio
async def test_poll(
    client: AsyncClient,
    logger: BoundLogger,
    uws_config: UWSConfig,
    uws_factory: UWSFactory,
    stub_worker: Worker,
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
    now = datetime.now(tz=timezone.utc)
    r = await client.get(
        "/jobs/1",
        headers={"X-Auth-Request-User": "user"},
        params={"WAIT": "2"},
    )
    assert (datetime.now(tz=timezone.utc) - now).total_seconds() >= 2
    assert r.status_code == 200
    assert r.text == PENDING_JOB.strip().format(
        "PENDING",
        isodate(job.creation_time),
        isodate(job.creation_time + timedelta(seconds=24 * 60 * 60)),
    )

    # Change the job to be one that waits for a couple of seconds and then
    # returns a result.
    def worker(
        params: List[JobParameter], logger: BoundLogger
    ) -> List[JobResult]:
        assert params == [JobParameter(parameter_id="id", value="bar")]
        time.sleep(2)
        return [JobResult(result_id="cutout", url="https://example.com/")]

    @dramatiq.actor(broker=uws_broker)
    def task(job_id: str) -> None:
        return uws_worker(job_id, uws_config, logger, worker)

    # Start the job and worker.
    uws_dependency.override_actor(task)
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
        isodate(job.creation_time),
        isodate(job.creation_time + timedelta(seconds=24 * 60 * 60)),
    )
    now = datetime.now(tz=timezone.utc)
    stub_worker.start()

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
            isodate(job.creation_time),
            isodate(job.start_time),
            isodate(job.creation_time + timedelta(seconds=24 * 60 * 60)),
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
            isodate(job.creation_time),
            isodate(job.start_time),
            isodate(job.end_time),
            isodate(job.creation_time + timedelta(seconds=24 * 60 * 60)),
        )
        assert (datetime.now(tz=timezone.utc) - now).total_seconds() >= 2
    finally:
        stub_worker.stop()
