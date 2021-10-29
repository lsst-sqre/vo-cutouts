"""Test handling of jobs that fail."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

import dramatiq
import pytest
from dramatiq import Worker
from dramatiq.middleware import CurrentMessage

from tests.support.uws import (
    TrivialPolicy,
    job_started,
    uws_broker,
    wait_for_job,
)
from vocutouts.uws.dependencies import uws_dependency
from vocutouts.uws.exceptions import TaskFatalError, TaskTransientError
from vocutouts.uws.models import ErrorCode, JobParameter
from vocutouts.uws.utils import isodatetime

if TYPE_CHECKING:
    from typing import Any, Dict, List

    from httpx import AsyncClient
    from structlog.stdlib import BoundLogger

    from vocutouts.uws.config import UWSConfig
    from vocutouts.uws.dependencies import UWSFactory


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

    # Create a backend worker that raises a transient error.
    @dramatiq.actor(broker=uws_broker, queue_name="job")
    def error_transient_job(job_id: str) -> List[Dict[str, Any]]:
        message = CurrentMessage.get_current_message()
        now = datetime.now(tz=timezone.utc)
        job_started.send(job_id, message.message_id, isodatetime(now))
        raise TaskTransientError(
            ErrorCode.USAGE_ERROR, "UsageError Something failed"
        )

    # Start the job.
    uws_dependency.override_policy(TrivialPolicy(error_transient_job))
    r = await client.post(
        "/jobs/1/phase",
        headers={"X-Auth-Request-User": "user"},
        data={"PHASE": "RUN"},
    )
    assert r.status_code == 303
    worker = Worker(uws_broker, worker_timeout=100)
    worker.start()

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
            isodatetime(job.creation_time),
            isodatetime(job.start_time),
            isodatetime(job.end_time),
            isodatetime(job.destruction_time),
            "transient",
            "false",
            "UsageError Something failed",
        )

        # Retrieve the error separately.
        r = await client.get(
            "/jobs/1/error", headers={"X-Auth-Request-User": "user"}
        )
        assert r.status_code == 200
        assert r.text == JOB_ERROR_SUMMARY.strip().format(
            "UsageError Something failed"
        )
    finally:
        worker.stop()


@pytest.mark.asyncio
async def test_fatal_error(
    client: AsyncClient,
    logger: BoundLogger,
    uws_config: UWSConfig,
    uws_factory: UWSFactory,
) -> None:
    job_service = uws_factory.create_job_service()
    job = await job_service.create(
        "user", params=[JobParameter(parameter_id="id", value="bar")]
    )

    # Create a backend worker that raises a fatal error with detail.
    @dramatiq.actor(broker=uws_broker, queue_name="job")
    def error_fatal_job(job_id: str) -> List[Dict[str, Any]]:
        message = CurrentMessage.get_current_message()
        now = datetime.now(tz=timezone.utc)
        job_started.send(job_id, message.message_id, isodatetime(now))
        raise TaskFatalError(ErrorCode.ERROR, "Error Whoops\nSome details")

    # Start the job.
    uws_dependency.override_policy(TrivialPolicy(error_fatal_job))
    r = await client.post(
        "/jobs/1/phase",
        headers={"X-Auth-Request-User": "user"},
        data={"PHASE": "RUN"},
    )
    assert r.status_code == 303
    worker = Worker(uws_broker, worker_timeout=100)
    worker.start()

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
            isodatetime(job.creation_time),
            isodatetime(job.start_time),
            isodatetime(job.end_time),
            isodatetime(job.destruction_time),
            "fatal",
            "true",
            "Error Whoops",
        )

        # Retrieve the error separately.
        r = await client.get(
            "/jobs/1/error", headers={"X-Auth-Request-User": "user"}
        )
        assert r.status_code == 200
        assert r.text == JOB_ERROR_SUMMARY.strip().format(
            "Error Whoops\n\nSome details"
        )
    finally:
        worker.stop()


@pytest.mark.asyncio
async def test_unknown_error(
    client: AsyncClient,
    logger: BoundLogger,
    uws_config: UWSConfig,
    uws_factory: UWSFactory,
) -> None:
    job_service = uws_factory.create_job_service()
    job = await job_service.create(
        "user", params=[JobParameter(parameter_id="id", value="bar")]
    )

    # Create a backend worker that raises a fatal error with detail.
    @dramatiq.actor(broker=uws_broker, queue_name="job")
    def error_unknown_job(job_id: str) -> List[Dict[str, Any]]:
        message = CurrentMessage.get_current_message()
        now = datetime.now(tz=timezone.utc)
        job_started.send(job_id, message.message_id, isodatetime(now))
        raise ValueError("Unknown exception")

    # Start the job.
    uws_dependency.override_policy(TrivialPolicy(error_unknown_job))
    r = await client.post(
        "/jobs/1/phase",
        headers={"X-Auth-Request-User": "user"},
        data={"PHASE": "RUN"},
    )
    assert r.status_code == 303
    worker = Worker(uws_broker, worker_timeout=100)
    worker.start()

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
            isodatetime(job.creation_time),
            isodatetime(job.start_time),
            isodatetime(job.end_time),
            isodatetime(job.destruction_time),
            "transient",
            "true",
            "Error Unknown error executing task",
        )

        # Retrieve the error separately.
        r = await client.get(
            "/jobs/1/error", headers={"X-Auth-Request-User": "user"}
        )
        assert r.status_code == 200
        assert r.text == JOB_ERROR_SUMMARY.strip().format(
            "Error Unknown error executing task\n\n"
            "ValueError: Unknown exception"
        )
    finally:
        worker.stop()
