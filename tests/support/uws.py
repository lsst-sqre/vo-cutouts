"""Support functions for testing UWS code."""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta
from typing import Annotated, Self

from arq.connections import RedisSettings
from fastapi import Form, Query
from pydantic import BaseModel, SecretStr
from safir.arq import ArqMode, JobMetadata, MockArqQueue

from vocutouts.uws.config import ParametersModel, UWSConfig
from vocutouts.uws.dependencies import UWSFactory
from vocutouts.uws.models import UWSJob, UWSJobParameter, UWSJobResult

__all__ = [
    "MockJobRunner",
    "SimpleParameters",
    "build_uws_config",
]


class SimpleWorkerParameters(BaseModel):
    name: str


class SimpleParameters(ParametersModel[SimpleWorkerParameters]):
    name: str

    @classmethod
    def from_job_parameters(cls, params: list[UWSJobParameter]) -> Self:
        assert len(params) == 1
        assert params[0].parameter_id == "name"
        return cls(name=params[0].value)

    def to_worker_parameters(self) -> SimpleWorkerParameters:
        return SimpleWorkerParameters(name=self.name)


async def _get_dependency(
    name: Annotated[str, Query()],
) -> list[UWSJobParameter]:
    return [UWSJobParameter(parameter_id="name", value=name, is_post=True)]


async def _post_dependency(
    name: Annotated[str, Form()],
) -> list[UWSJobParameter]:
    return [UWSJobParameter(parameter_id="name", value=name, is_post=True)]


def build_uws_config() -> UWSConfig:
    """Set up a test configuration.

    Exepcts the database hostname and port and the Redis hostname and port to
    be set in the environment following the conventions used by tox-docker,
    plus ``POSTGRES_USER``, ``POSTGRES_DB``, and ``POSTGRES_PASSWORD`` to
    specify the username, database, and password.
    """
    db_host = os.environ["POSTGRES_HOST"]
    db_port = os.environ["POSTGRES_5432_TCP_PORT"]
    db_user = os.environ["POSTGRES_USER"]
    db_name = os.environ["POSTGRES_DB"]
    database_url = f"postgresql://{db_user}@{db_host}:{db_port}/{db_name}"
    return UWSConfig(
        arq_mode=ArqMode.test,
        arq_redis_settings=RedisSettings(
            host=os.environ["REDIS_HOST"],
            port=int(os.environ["REDIS_6379_TCP_PORT"]),
        ),
        async_post_dependency=_post_dependency,
        database_url=database_url,
        database_password=SecretStr(os.environ["POSTGRES_PASSWORD"]),
        lifetime=timedelta(days=1),
        parameters_type=SimpleParameters,
        signing_service_account="signer@example.com",
        slack_webhook=SecretStr("https://example.com/fake-webhook"),
        sync_get_dependency=_get_dependency,
        sync_post_dependency=_post_dependency,
        worker="hello",
    )


class MockJobRunner:
    """Simulate execution of jobs with a mock queue.

    When running the test suite, the arq queue is replaced with a mock queue
    that doesn't execute workers. That execution has to be simulated by
    manually updating state in the mock queue and running the UWS database
    worker functions that normally would be run automatically by the queue.

    This class wraps that functionality. An instance of it is normally
    provided as a fixture, initialized with the same test objects as the test
    suite.

    Parameters
    ----------
    factory
        Factory for UWS components.
    arq_queue
        Mock arq queue for testing.
    """

    def __init__(self, factory: UWSFactory, arq_queue: MockArqQueue) -> None:
        self._service = factory.create_job_service()
        self._store = factory.create_job_store()
        self._arq = arq_queue

    async def get_job_metadata(
        self, username: str, job_id: str
    ) -> JobMetadata:
        """Get the arq job metadata for a job.

        Parameters
        ----------
        job_id
            UWS job ID.

        Returns
        -------
        JobMetadata
            arq job metadata.
        """
        job = await self._service.get(username, job_id)
        assert job.message_id
        return await self._arq.get_job_metadata(job.message_id)

    async def mark_in_progress(
        self, username: str, job_id: str, *, delay: float | None = None
    ) -> UWSJob:
        """Mark a queued job in progress.

        Parameters
        ----------
        username
            Owner of job.
        job_id
            Job ID.
        delay
            How long to delay in seconds before marking the job as complete.

        Returns
        -------
        UWSJob
            Record of the job.
        """
        if delay:
            await asyncio.sleep(delay)
        job = await self._service.get(username, job_id)
        assert job.message_id
        await self._arq.set_in_progress(job.message_id)
        await self._store.mark_executing(job_id, datetime.now(tz=UTC))
        return await self._service.get(username, job_id)

    async def mark_complete(
        self,
        username: str,
        job_id: str,
        results: list[UWSJobResult] | Exception,
        *,
        delay: float | None = None,
    ) -> UWSJob:
        """Mark an in progress job as complete.

        Parameters
        ----------
        username
            Owner of job.
        job_id
            Job ID.
        results
            Results to return. May be an exception to simulate a job failure.
        delay
            How long to delay in seconds before marking the job as complete.

        Returns
        -------
        UWSJob
            Record of the job.
        """
        if delay:
            await asyncio.sleep(delay)
        job = await self._service.get(username, job_id)
        assert job.message_id
        await self._arq.set_complete(job.message_id, result=results)
        job_result = await self._arq.get_job_result(job.message_id)
        await self._store.mark_completed(job_id, job_result)
        return await self._service.get(username, job_id)
