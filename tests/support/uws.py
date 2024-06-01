"""Support functions for testing UWS code."""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from arq.connections import RedisSettings
from pydantic import SecretStr
from safir.arq import ArqMode, ArqQueue, JobMetadata

from vocutouts.uws.config import UWSConfig
from vocutouts.uws.models import UWSJob, UWSJobParameter
from vocutouts.uws.policy import UWSPolicy

__all__ = [
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
        execution_duration=timedelta(minutes=10),
        lifetime=timedelta(days=1),
        database_url=database_url,
        database_password=SecretStr(os.environ["POSTGRES_PASSWORD"]),
        arq_mode=ArqMode.test,
        arq_redis_settings=RedisSettings(
            host=os.environ["REDIS_HOST"],
            port=int(os.environ["REDIS_6379_TCP_PORT"]),
        ),
        signing_service_account="",
    )
