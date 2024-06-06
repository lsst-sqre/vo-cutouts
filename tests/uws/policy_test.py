"""Test for the UWS policy layer."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest
from httpx import AsyncClient
from safir.arq import MockArqQueue
from safir.datetime import current_datetime, isodatetime

from vocutouts.uws.config import UWSConfig
from vocutouts.uws.dependencies import UWSFactory, uws_dependency
from vocutouts.uws.exceptions import ParameterError
from vocutouts.uws.models import UWSJob, UWSJobParameter

from ..support.uws import TrivialPolicy


class Policy(TrivialPolicy):
    def validate_destruction(
        self, destruction: datetime, job: UWSJob
    ) -> datetime:
        max_destruction = current_datetime() + timedelta(days=1)
        if destruction > max_destruction:
            return max_destruction
        else:
            return destruction

    def validate_execution_duration(
        self, execution_duration: int, job: UWSJob
    ) -> int:
        if execution_duration > 200:
            return 200
        else:
            return execution_duration

    def validate_params(self, params: list[UWSJobParameter]) -> None:
        for param in params:
            if param.parameter_id != "id":
                msg = f"Invalid parameter f{param.parameter_id}"
                raise ParameterError(msg)


@pytest.mark.asyncio
async def test_policy(
    client: AsyncClient,
    arq_queue: MockArqQueue,
    uws_factory: UWSFactory,
    uws_config: UWSConfig,
) -> None:
    policy = Policy(arq_queue, "trivial")
    uws_dependency.override_policy(policy)
    uws_factory._policy = policy
    job_service = uws_factory.create_job_service()

    # Check parameter rejection.
    with pytest.raises(ParameterError):
        await job_service.create(
            "user", params=[UWSJobParameter(parameter_id="foo", value="bar")]
        )

    # Create a job that should pass the policy layer.
    await job_service.create(
        "user", params=[UWSJobParameter(parameter_id="id", value="bar")]
    )

    # Change the destruction time, first to something that should be honored
    # and then something that should be overridden.
    destruction = current_datetime() + timedelta(hours=1)
    r = await client.post(
        "/jobs/1/destruction",
        headers={"X-Auth-Request-User": "user"},
        data={"desTRUcTiON": isodatetime(destruction)},
    )
    assert r.status_code == 303
    assert r.headers["Location"] == "https://example.com/jobs/1"
    r = await client.get(
        "/jobs/1/destruction", headers={"X-Auth-Request-User": "user"}
    )
    assert r.status_code == 200
    assert r.text == isodatetime(destruction)
    destruction = current_datetime() + timedelta(days=5)
    expected = current_datetime() + timedelta(days=1)
    r = await client.post(
        "/jobs/1/destruction",
        headers={"X-Auth-Request-User": "user"},
        data={"destruction": isodatetime(destruction)},
    )
    assert r.status_code == 303
    assert r.headers["Location"] == "https://example.com/jobs/1"
    r = await client.get(
        "/jobs/1/destruction", headers={"X-Auth-Request-User": "user"}
    )
    assert r.status_code == 200
    seen = datetime.fromisoformat(r.text[:-1] + "+00:00")
    assert seen >= expected - timedelta(seconds=5)
    assert seen <= expected + timedelta(seconds=5)

    # Now do the same thing for execution duration.
    r = await client.post(
        "/jobs/1/executionduration",
        headers={"X-Auth-Request-User": "user"},
        data={"exECUTionduRATION": 100},
    )
    assert r.status_code == 303
    assert r.headers["Location"] == "https://example.com/jobs/1"
    r = await client.get(
        "/jobs/1/executionduration", headers={"X-Auth-Request-User": "user"}
    )
    assert r.status_code == 200
    assert r.text == "100"
    r = await client.post(
        "/jobs/1/executionduration",
        headers={"X-Auth-Request-User": "user"},
        data={"exECUTionduRATION": 250},
    )
    assert r.status_code == 303
    assert r.headers["Location"] == "https://example.com/jobs/1"
    r = await client.get(
        "/jobs/1/executionduration", headers={"X-Auth-Request-User": "user"}
    )
    assert r.status_code == 200
    assert r.text == "200"
