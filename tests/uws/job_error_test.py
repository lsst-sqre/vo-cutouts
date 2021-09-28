"""Test handling of jobs that fail."""

from __future__ import annotations

from typing import TYPE_CHECKING

import dramatiq
import pytest

from tests.support.uws import uws_broker, wait_for_job
from vocutouts.uws.dependencies import uws_dependency
from vocutouts.uws.exceptions import TaskFatalError, TaskTransientError
from vocutouts.uws.models import JobParameter
from vocutouts.uws.tasks import uws_worker
from vocutouts.uws.utils import isodate

if TYPE_CHECKING:
    from typing import List

    from dramatiq import Worker
    from httpx import AsyncClient
    from structlog.stdlib import BoundLogger

    from vocutouts.uws.config import UWSConfig
    from vocutouts.uws.dependencies import UWSFactory
    from vocutouts.uws.models import JobResult


ERRORED_JOB = """
<uws:job
    version="1.1"
    xsi:schemaLocation="http://www.ivoa.net/xml/UWS/v1.0 UWS.xsd"
    xmlns:xml="http://www.w3.org/XML/1998/namespace"
    xmlns:uws="http://www.ivoa.net/xml/UWS/v1.0"
    xmlns:xlink="http://www.w3.org/1999/xlink"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <uws:jobId>1</uws:jobId>
  <uws:ownerId>user</uws:ownerId>
  <uws:phase>ERROR</uws:phase>
  <uws:creationTime>{}</uws:creationTime>
  <uws:startTime>{}</uws:startTime>
  <uws:endTime>{}</uws:endTime>
  <uws:executionDuration>600</uws:executionDuration>
  <uws:destruction>{}</uws:destruction>
  <uws:parameters>
    <uws:parameter id="id">bar</uws:parameter>
  </uws:parameters>
  <uws:errorSummary type="{}" hasDetail="{}">
    <uws:message>{}</uws:message>
  </uws:errorSummary>
</uws:job>
"""

JOB_ERROR_SUMMARY = """
<?xml version="1.0" encoding="UTF-8"?>
<VOTABLE version="1.4" xmlns="http://www.ivoa.net/xml/VOTable/v1.3">
  <INFO name="QUERY_STATUS" value="ERROR">
{}
  </INFO>
</VOTABLE>
"""


@pytest.mark.asyncio
async def test_temporary_error(
    client: AsyncClient,
    logger: BoundLogger,
    uws_config: UWSConfig,
    uws_factory: UWSFactory,
    stub_worker: Worker,
) -> None:
    job_service = uws_factory.create_job_service()
    job = await job_service.create(
        "user", params=[JobParameter(parameter_id="id", value="bar")]
    )

    # The pending job has no error.
    r = await client.get(
        "/jobs/1/error", headers={"X-Auth-Request-User": "user"}
    )
    assert r.status_code == 404

    # Create a backend worker that raises a temporary error with no detail.
    def worker(
        params: List[JobParameter], logger: BoundLogger
    ) -> List[JobResult]:
        raise TaskTransientError("Something failed")

    @dramatiq.actor(broker=uws_broker)
    def error_task(job_id: str) -> None:
        return uws_worker(job_id, uws_config, logger, worker)

    # Start the job.
    uws_dependency.override_actor(error_task)
    r = await client.post(
        "/jobs/1/phase",
        headers={"X-Auth-Request-User": "user"},
        data={"PHASE": "RUN"},
    )
    assert r.status_code == 303
    stub_worker.start()

    # Check the results.
    try:
        job = await wait_for_job(job_service, "user", "1")
        assert job.start_time
        assert job.end_time
        r = await client.get(
            "/jobs/1", headers={"X-Auth-Request-User": "user"}
        )
        assert r.status_code == 200
        assert r.text == ERRORED_JOB.strip().format(
            isodate(job.creation_time),
            isodate(job.start_time),
            isodate(job.end_time),
            isodate(job.destruction_time),
            "transient",
            "false",
            "Something failed",
        )

        # Retrieve the error separately.
        r = await client.get(
            "/jobs/1/error", headers={"X-Auth-Request-User": "user"}
        )
        assert r.status_code == 200
        assert r.text == JOB_ERROR_SUMMARY.strip().format("Something failed")
    finally:
        stub_worker.stop()


@pytest.mark.asyncio
async def test_fatal_error(
    client: AsyncClient,
    logger: BoundLogger,
    uws_config: UWSConfig,
    uws_factory: UWSFactory,
    stub_worker: Worker,
) -> None:
    job_service = uws_factory.create_job_service()
    job = await job_service.create(
        "user", params=[JobParameter(parameter_id="id", value="bar")]
    )

    # Create a backend worker that raises a fatal error with detail.
    def worker(
        params: List[JobParameter], logger: BoundLogger
    ) -> List[JobResult]:
        raise TaskFatalError("Whoops", detail="Some details")

    @dramatiq.actor(broker=uws_broker)
    def error_task(job_id: str) -> None:
        return uws_worker(job_id, uws_config, logger, worker)

    # Start the job.
    uws_dependency.override_actor(error_task)
    r = await client.post(
        "/jobs/1/phase",
        headers={"X-Auth-Request-User": "user"},
        data={"PHASE": "RUN"},
    )
    assert r.status_code == 303
    stub_worker.start()

    # Check the results.
    try:
        job = await wait_for_job(job_service, "user", "1")
        assert job.start_time
        assert job.end_time
        r = await client.get(
            "/jobs/1", headers={"X-Auth-Request-User": "user"}
        )
        assert r.status_code == 200
        assert r.text == ERRORED_JOB.strip().format(
            isodate(job.creation_time),
            isodate(job.start_time),
            isodate(job.end_time),
            isodate(job.destruction_time),
            "fatal",
            "true",
            "Whoops",
        )

        # Retrieve the error separately.
        r = await client.get(
            "/jobs/1/error", headers={"X-Auth-Request-User": "user"}
        )
        assert r.status_code == 200
        assert r.text == JOB_ERROR_SUMMARY.strip().format(
            "Whoops\n\nSome details"
        )
    finally:
        stub_worker.stop()


@pytest.mark.asyncio
async def test_unknown_error(
    client: AsyncClient,
    logger: BoundLogger,
    uws_config: UWSConfig,
    uws_factory: UWSFactory,
    stub_worker: Worker,
) -> None:
    job_service = uws_factory.create_job_service()
    job = await job_service.create(
        "user", params=[JobParameter(parameter_id="id", value="bar")]
    )

    # Create a backend worker that raises a fatal error with detail.
    def worker(
        params: List[JobParameter], logger: BoundLogger
    ) -> List[JobResult]:
        raise ValueError("Unknown exception")

    @dramatiq.actor(broker=uws_broker)
    def error_task(job_id: str) -> None:
        return uws_worker(job_id, uws_config, logger, worker)

    # Start the job.
    uws_dependency.override_actor(error_task)
    r = await client.post(
        "/jobs/1/phase",
        headers={"X-Auth-Request-User": "user"},
        data={"PHASE": "RUN"},
    )
    assert r.status_code == 303
    stub_worker.start()

    # Check the results.
    try:
        job = await wait_for_job(job_service, "user", "1")
        assert job.start_time
        assert job.end_time
        r = await client.get(
            "/jobs/1", headers={"X-Auth-Request-User": "user"}
        )
        assert r.status_code == 200
        assert r.text == ERRORED_JOB.strip().format(
            isodate(job.creation_time),
            isodate(job.start_time),
            isodate(job.end_time),
            isodate(job.destruction_time),
            "transient",
            "true",
            "Unknown error executing task",
        )

        # Retrieve the error separately.
        r = await client.get(
            "/jobs/1/error", headers={"X-Auth-Request-User": "user"}
        )
        assert r.status_code == 200
        assert r.text == JOB_ERROR_SUMMARY.strip().format(
            "Unknown error executing task\n\nValueError: Unknown exception"
        )
    finally:
        stub_worker.stop()
