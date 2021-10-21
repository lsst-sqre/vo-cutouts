"""Tests for the UWS job manipulation handlers.

These tests don't assume any given application, and therefore don't use the
API to create a job, instead inserting it directly via the UWSService.
"""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

import pytest
from dramatiq import Worker

from tests.support.uws import uws_broker, wait_for_job
from vocutouts.uws.models import JobParameter
from vocutouts.uws.utils import isodatetime

if TYPE_CHECKING:
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
  <uws:jobId>{}</uws:jobId>
  <uws:runId>some-run-id</uws:runId>
  <uws:ownerId>user</uws:ownerId>
  <uws:phase>{}</uws:phase>
  <uws:creationTime>{}</uws:creationTime>
  <uws:executionDuration>{}</uws:executionDuration>
  <uws:destruction>{}</uws:destruction>
  <uws:parameters>
    <uws:parameter id="id">bar</uws:parameter>
    <uws:parameter id="circle">1 1 1</uws:parameter>
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
  <uws:runId>some-run-id</uws:runId>
  <uws:ownerId>user</uws:ownerId>
  <uws:phase>COMPLETED</uws:phase>
  <uws:creationTime>{}</uws:creationTime>
  <uws:startTime>{}</uws:startTime>
  <uws:endTime>{}</uws:endTime>
  <uws:executionDuration>600</uws:executionDuration>
  <uws:destruction>{}</uws:destruction>
  <uws:parameters>
    <uws:parameter id="id">bar</uws:parameter>
    <uws:parameter id="circle">1 1 1</uws:parameter>
  </uws:parameters>
  <uws:results>
    <uws:result id="cutout" xlink:href="https://example.com/cutout-result"\
 mime-type="application/fits"/>
  </uws:results>
</uws:job>
"""

JOB_PARAMETERS = """
<uws:parameters
    version="1.1"
    xsi:schemaLocation="http://www.ivoa.net/xml/UWS/v1.0 UWS.xsd"
    xmlns:xml="http://www.w3.org/XML/1998/namespace"
    xmlns:uws="http://www.ivoa.net/xml/UWS/v1.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <uws:parameter id="id">bar</uws:parameter>
  <uws:parameter id="circle">1 1 1</uws:parameter>
</uws:parameters>
"""

JOB_RESULTS = """
<uws:results
    version="1.1"
    xsi:schemaLocation="http://www.ivoa.net/xml/UWS/v1.0 UWS.xsd"
    xmlns:xml="http://www.w3.org/XML/1998/namespace"
    xmlns:uws="http://www.ivoa.net/xml/UWS/v1.0"
    xmlns:xlink="http://www.w3.org/1999/xlink"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <uws:result id="cutout" xlink:href="https://example.com/cutout-result"\
 mime-type="application/fits"/>
</uws:results>
"""


@pytest.mark.asyncio
async def test_job_run(
    client: AsyncClient,
    logger: BoundLogger,
    uws_config: UWSConfig,
    uws_factory: UWSFactory,
) -> None:
    job_service = uws_factory.create_job_service()
    job = await job_service.create(
        "user",
        run_id="some-run-id",
        params=[
            JobParameter(parameter_id="id", value="bar"),
            JobParameter(parameter_id="circle", value="1 1 1"),
        ],
    )

    # Check the retrieval of the job configuration.
    r = await client.get("/jobs/1", headers={"X-Auth-Request-User": "user"})
    assert r.status_code == 200
    assert r.headers["Content-Type"] == "application/xml"
    assert r.text == PENDING_JOB.strip().format(
        "1",
        "PENDING",
        isodatetime(job.creation_time),
        "600",
        isodatetime(job.creation_time + timedelta(seconds=24 * 60 * 60)),
    )

    # Try to put the job in an invalid phase.
    r = await client.post(
        "/jobs/1/phase",
        headers={"X-Auth-Request-User": "user"},
        data={"PHASE": "EXECUTING"},
    )
    assert r.status_code == 422
    assert r.text.startswith("UsageError")

    # Aborting jobs is not supported.
    r = await client.post(
        "/jobs/1/phase",
        headers={"X-Auth-Request-User": "user"},
        data={"PHASE": "ABORT"},
    )
    assert r.status_code == 403
    assert r.text.startswith("AuthorizationError")

    # Start the job.
    r = await client.post(
        "/jobs/1/phase",
        headers={"X-Auth-Request-User": "user"},
        data={"PHASE": "RUN"},
        follow_redirects=True,
    )
    assert r.status_code == 200
    assert r.url == "https://example.com/jobs/1"
    assert r.text == PENDING_JOB.strip().format(
        "1",
        "QUEUED",
        isodatetime(job.creation_time),
        "600",
        isodatetime(job.creation_time + timedelta(seconds=24 * 60 * 60)),
    )

    # Start the job worker.
    worker = Worker(uws_broker, worker_timeout=100)
    worker.start()

    # Check the job results.
    try:
        job = await wait_for_job(job_service, "user", "1")
        assert job.start_time
        assert job.end_time
        assert job.end_time >= job.start_time >= job.creation_time
        r = await client.get(
            "/jobs/1", headers={"X-Auth-Request-User": "user"}
        )
        assert r.status_code == 200
        assert r.headers["Content-Type"] == "application/xml"
        assert r.text == FINISHED_JOB.strip().format(
            isodatetime(job.creation_time),
            isodatetime(job.start_time),
            isodatetime(job.end_time),
            isodatetime(job.creation_time + timedelta(seconds=24 * 60 * 60)),
        )

        # Check that the phase is now correct.
        r = await client.get(
            "/jobs/1/phase", headers={"X-Auth-Request-User": "user"}
        )
        assert r.status_code == 200
        assert r.headers["Content-Type"] == "text/plain; charset=utf-8"
        assert r.text == "COMPLETED"

        # Retrieve them directly through the results resource.
        r = await client.get(
            "/jobs/1/results", headers={"X-Auth-Request-User": "user"}
        )
        assert r.status_code == 200
        assert r.headers["Content-Type"] == "application/xml"
        assert r.text == JOB_RESULTS.strip()

        # There should be no error message.
        r = await client.get(
            "/jobs/1/error", headers={"X-Auth-Request-User": "user"}
        )
        assert r.status_code == 404
    finally:
        worker.stop()


@pytest.mark.asyncio
async def test_job_api(
    client: AsyncClient,
    uws_factory: UWSFactory,
) -> None:
    job_service = uws_factory.create_job_service()
    job = await job_service.create(
        "user",
        run_id="some-run-id",
        params=[
            JobParameter(parameter_id="id", value="bar"),
            JobParameter(parameter_id="circle", value="1 1 1"),
        ],
    )

    # Check the retrieval of the job configuration.
    destruction_time = job.creation_time + timedelta(seconds=24 * 60 * 60)
    r = await client.get("/jobs/1", headers={"X-Auth-Request-User": "user"})
    assert r.status_code == 200
    assert r.headers["Content-Type"] == "application/xml"
    assert r.text == PENDING_JOB.strip().format(
        "1",
        "PENDING",
        isodatetime(job.creation_time),
        "600",
        isodatetime(destruction_time),
    )

    # Check retrieving each part separately.
    r = await client.get(
        "/jobs/1/destruction", headers={"X-Auth-Request-User": "user"}
    )
    assert r.status_code == 200
    assert r.headers["Content-Type"] == "text/plain; charset=utf-8"
    assert r.text == isodatetime(destruction_time)

    r = await client.get(
        "/jobs/1/executionduration", headers={"X-Auth-Request-User": "user"}
    )
    assert r.status_code == 200
    assert r.headers["Content-Type"] == "text/plain; charset=utf-8"
    assert r.text == "600"

    r = await client.get(
        "/jobs/1/owner", headers={"X-Auth-Request-User": "user"}
    )
    assert r.status_code == 200
    assert r.headers["Content-Type"] == "text/plain; charset=utf-8"
    assert r.text == "user"

    r = await client.get(
        "/jobs/1/parameters", headers={"X-Auth-Request-User": "user"}
    )
    assert r.status_code == 200
    assert r.headers["Content-Type"] == "application/xml"
    assert r.text == JOB_PARAMETERS.strip()

    r = await client.get(
        "/jobs/1/phase", headers={"X-Auth-Request-User": "user"}
    )
    assert r.status_code == 200
    assert r.headers["Content-Type"] == "text/plain; charset=utf-8"
    assert r.text == "PENDING"

    r = await client.get(
        "/jobs/1/quote", headers={"X-Auth-Request-User": "user"}
    )
    assert r.status_code == 200
    assert r.headers["Content-Type"] == "text/plain; charset=utf-8"
    assert r.text == ""

    # Modify various settings.  These go through the policy layer, which is
    # mocked to do nothing.  Policy rejections will be tested elsewhere.
    r = await client.post(
        "/jobs/1/destruction",
        headers={"X-Auth-Request-User": "user"},
        data={"DESTRUCTION": "2021-09-10T10:01:02Z"},
    )
    assert r.status_code == 303
    assert r.headers["Location"] == "https://example.com/jobs/1"

    r = await client.post(
        "/jobs/1/executionduration",
        headers={"X-Auth-Request-User": "user"},
        data={"ExecutionDuration": 1200},
    )
    assert r.status_code == 303
    assert r.headers["Location"] == "https://example.com/jobs/1"

    # Retrieve the modified job and check that the new values are recorded.
    r = await client.get("/jobs/1", headers={"X-Auth-Request-User": "user"})
    assert r.status_code == 200
    assert r.headers["Content-Type"] == "application/xml"
    assert r.text == PENDING_JOB.strip().format(
        "1",
        "PENDING",
        isodatetime(job.creation_time),
        "1200",
        "2021-09-10T10:01:02Z",
    )

    # Delete the job.
    r = await client.delete("/jobs/1", headers={"X-Auth-Request-User": "user"})
    assert r.status_code == 303
    assert r.headers["Location"] == "https://example.com/jobs"
    r = await client.get("/jobs/1", headers={"X-Auth-Request-User": "user"})
    assert r.status_code == 404

    # Create a new job and then delete it via POST.
    job = await job_service.create(
        "user",
        run_id="some-run-id",
        params=[
            JobParameter(parameter_id="id", value="bar"),
            JobParameter(parameter_id="circle", value="1 1 1"),
        ],
    )
    r = await client.get("/jobs/2", headers={"X-Auth-Request-User": "user"})
    assert r.status_code == 200
    assert r.text == PENDING_JOB.strip().format(
        "2",
        "PENDING",
        isodatetime(job.creation_time),
        "600",
        isodatetime(job.destruction_time),
    )
    r = await client.post(
        "/jobs/2",
        headers={"X-Auth-Request-User": "user"},
        data={"ACTION": "DELETE"},
    )
    assert r.status_code == 303
    assert r.headers["Location"] == "https://example.com/jobs"
    r = await client.get("/jobs/2", headers={"X-Auth-Request-User": "user"})
    assert r.status_code == 404
