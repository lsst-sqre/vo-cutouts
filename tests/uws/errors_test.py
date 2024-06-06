"""Tests for errors from the UWS API."""

from __future__ import annotations

from dataclasses import dataclass

import pytest
from httpx import AsyncClient
from safir.testing.slack import MockSlackWebhook

from vocutouts.uws.dependencies import UWSFactory
from vocutouts.uws.models import UWSJobParameter


@dataclass
class PostTest:
    """Encapsulates the data a test POST."""

    url: str
    data: dict[str, str]


@pytest.mark.asyncio
async def test_errors(
    client: AsyncClient, uws_factory: UWSFactory, mock_slack: MockSlackWebhook
) -> None:
    job_service = uws_factory.create_job_service()
    await job_service.create(
        "user",
        run_id="some-run-id",
        params=[
            UWSJobParameter(parameter_id="id", value="bar"),
            UWSJobParameter(parameter_id="circle", value="1 1 1"),
        ],
    )

    # No user specified.
    routes = [
        "/jobs/1",
        "/jobs/1/destruction",
        "/jobs/1/error",
        "/jobs/1/executionduration",
        "/jobs/1/owner",
        "/jobs/1/parameters",
        "/jobs/1/phase",
        "/jobs/1/quote",
        "/jobs/1/results",
    ]
    for route in routes:
        r = await client.get(route)
        assert r.status_code == 422
        assert r.text.startswith("UsageError")

    # Wrong user specified.
    for route in routes:
        r = await client.get(
            route, headers={"X-Auth-Request-User": "otheruser"}
        )
        assert r.status_code == 403
        assert r.text.startswith("AuthorizationError")

    # Job does not exist.
    for route in (r.replace("/1", "/2") for r in routes):
        r = await client.get(route, headers={"X-Auth-Request-User": "user"})
        assert r.status_code == 404
        assert r.text.startswith("UsageError")

    # Check no user specified with POST routes.
    tests = [
        PostTest("/jobs/1", {"action": "DELETE"}),
        PostTest(
            "/jobs/1/destruction", {"destruction": "2021-09-10T10:01:02Z"}
        ),
        PostTest("/jobs/1/executionduration", {"executionduration": "1200"}),
        PostTest("/jobs/1/phase", {"phase": "RUN"}),
    ]
    for test in tests:
        r = await client.post(test.url, data=test.data)
        assert r.status_code == 422
        assert r.text.startswith("UsageError")

    # Wrong user specified.
    for test in tests:
        r = await client.post(
            test.url,
            data=test.data,
            headers={"X-Auth-Request-User": "otheruser"},
        )
        assert r.status_code == 403
        assert r.text.startswith("AuthorizationError")

    # Job does not exist.
    for test in tests:
        url = test.url.replace("/1", "/2")
        r = await client.post(
            url, data=test.data, headers={"X-Auth-Request-User": "user"}
        )
        assert r.status_code == 404
        assert r.text.startswith("UsageError")

    # Finally, test all the same things with the one supported DELETE.
    r = await client.delete("/jobs/1")
    assert r.status_code == 422
    assert r.text.startswith("UsageError")
    r = await client.delete(
        "/jobs/1", headers={"X-Auth-Request-User": "otheruser"}
    )
    assert r.status_code == 403
    assert r.text.startswith("AuthorizationError")
    r = await client.delete("/jobs/2", headers={"X-Auth-Request-User": "user"})
    assert r.status_code == 404
    assert r.text.startswith("UsageError")

    # Try some bogus destruction and execution duration parameters.
    tests = [
        PostTest("/jobs/1/destruction", {"destruction": "next tuesday"}),
        PostTest("/jobs/1/destruction", {"DESTruction": "next tuesday"}),
        PostTest(
            "/jobs/1/destruction", {"destruction": "2021-09-10T10:01:02"}
        ),
        PostTest(
            "/jobs/1/destruction", {"destrucTION": "2021-09-10T10:01:02"}
        ),
        PostTest("/jobs/1/executionduration", {"executionduration": "0"}),
        PostTest("/jobs/1/executionduration", {"executionDUration": "0"}),
        PostTest("/jobs/1/executionduration", {"executionduration": "-1"}),
        PostTest("/jobs/1/executionduration", {"executionDUration": "-1"}),
        PostTest("/jobs/1/executionduration", {"executionduration": "fred"}),
        PostTest("/jobs/1/executionduration", {"executionDUration": "fred"}),
    ]
    for test in tests:
        r = await client.post(
            test.url, data=test.data, headers={"X-Auth-Request-User": "user"}
        )
        assert r.status_code == 422
        assert r.text.startswith("UsageError")

    # None of these errors should have produced Slack errors.
    assert mock_slack.messages == []
