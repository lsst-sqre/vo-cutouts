"""Support functions for testing UWS code."""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta

from arq.connections import RedisSettings
from pydantic import SecretStr
from safir.arq import ArqMode, ArqQueue, JobMetadata, MockArqQueue

from vocutouts.uws.config import UWSConfig
from vocutouts.uws.dependencies import UWSFactory
from vocutouts.uws.models import UWSJob, UWSJobParameter, UWSJobResult
from vocutouts.uws.policy import UWSPolicy

__all__ = [
    "MockJobRunner",
    "TrivialPolicy",
    "build_uws_config",
]


class TrivialPolicy(UWSPolicy):
    """Trivial UWS policy that calls a worker with only one parameter.

    Parameters
    ----------
    arq
        Underlying arq queue.
    function
        Name of the function to run.
    """

    def __init__(self, arq: ArqQueue, function: str) -> None:
        super().__init__(arq)
        self._function = function

    async def dispatch(self, job: UWSJob, access_token: str) -> JobMetadata:
        return await self.arq.enqueue(self._function, job.job_id)

    def validate_destruction(
        self, destruction: datetime, job: UWSJob
    ) -> datetime:
        return destruction

    def validate_execution_duration(
        self, execution_duration: int, job: UWSJob
    ) -> int:
        return execution_duration

    def validate_params(self, params: list[UWSJobParameter]) -> None:
        pass


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
        execution_duration=timedelta(minutes=10),
        lifetime=timedelta(days=1),
        database_url=database_url,
        database_password=SecretStr(os.environ["POSTGRES_PASSWORD"]),
        signing_service_account="",
        slack_webhook=SecretStr("https://example.com/fake-webhook"),
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
