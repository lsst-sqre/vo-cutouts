"""Configuration for the UWS service."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Self, TypeAlias

from arq.connections import RedisSettings
from pydantic import BaseModel, SecretStr
from safir.arq import ArqMode

from .models import UWSJob, UWSJobParameter

DestructionValidator: TypeAlias = Callable[[datetime, UWSJob], datetime]
"""Type for a validator for a new destruction time."""

ExecutionDurationValidator: TypeAlias = Callable[
    [timedelta, UWSJob], timedelta
]
"""Type for a validator for a new execution duration."""

ParametersDependency: TypeAlias = Callable[
    ..., Coroutine[None, None, list[UWSJobParameter]]
]
"""Type for a dependency that gathers parameters for a job."""

__all__ = [
    "DestructionValidator",
    "ExecutionDurationValidator",
    "ParametersDependency",
    "ParametersModel",
    "UWSConfig",
]


class ParametersModel(BaseModel, ABC):
    """Defines the interface for a model suitable for job parameters."""

    @classmethod
    @abstractmethod
    def from_job_parameters(cls, params: list[UWSJobParameter]) -> Self:
        """Validate generic UWS parameters and convert to the internal model.

        Parameters
        ----------
        params
            Generic input job parameters.

        Returns
        -------
        ParametersModel
            Parsed cutout parameters specific to service.

        Raises
        ------
        vocutouts.uws.exceptions.MultiValuedParameterError
            Raised if multiple parameters are provided but not supported.
        vocutouts.uws.exceptions.ParameterError
            Raised if one of the parameters could not be parsed.
        pydantic.ValidationError
            Raised if the parameters do not validate.
        """


@dataclass
class UWSConfig:
    """Configuration for the UWS service.

    The UWS service may be embedded in a variety of VO applications. This
    class encapsulates the configuration of the UWS component that may vary by
    service or specific installation.
    """

    arq_mode: ArqMode
    """What mode to use for the arq queue."""

    arq_redis_settings: RedisSettings
    """Settings for Redis for the arq queue."""

    async_post_dependency: ParametersDependency
    """Dependency for job parameters for an async job via POST.

    This FastAPI should expect POST parameters and return a list of
    `~vocutouts.uws.models.UWSJobParameter` objects representing the job
    parameters.
    """

    database_url: str
    """URL for the metadata database."""

    execution_duration: timedelta
    """Maximum execution time in seconds.

    Jobs that run longer than this length of time will be automatically
    aborted.
    """

    lifetime: timedelta
    """The lifetime of jobs.

    After this much time elapses since the creation of the job, all of the
    results from the job will be cleaned up and all record of the job will be
    deleted.
    """

    parameters_type: type[ParametersModel]
    """Type representing the job parameters.

    This will be used to validate parameters and to parse them before passing
    them to the worker.
    """

    signing_service_account: str
    """Email of service account to use for signed URLs.

    The default credentials that the application frontend runs with must have
    the ``roles/iam.serviceAccountTokenCreator`` role on the service account
    with this email.
    """

    worker: str
    """Name of the backend worker to call to execute a job."""

    database_password: SecretStr | None = None
    """Password for the database."""

    slack_webhook: SecretStr | None = None
    """Slack incoming webhook for reporting errors."""

    sync_get_dependency: ParametersDependency | None = None
    """Dependency for job parameters for a sync job via GET.

    This FastAPI should expect GET parameters and return a list of
    `~vocutouts.uws.models.UWSJobParameter` objects representing the job
    parameters. If `None`, no route to create a job via sync GET will be
    created.
    """

    sync_post_dependency: ParametersDependency | None = None
    """Dependency for job parameters for a sync job via POST.

    This FastAPI should expect POST parameters and return a list of
    `~vocutouts.uws.models.UWSJobParameter` objects representing the job
    parameters. If `None`, no route to create a job via sync POST will be
    created.
    """

    sync_timeout: timedelta = timedelta(minutes=5)
    """Maximum lifetime of a sync request."""

    url_lifetime: timedelta = timedelta(minutes=15)
    """How long result URLs should be valid for."""

    validate_destruction: DestructionValidator | None = None
    """Validate a new destruction time for a job.

    If provided, called with the requested destruction time and the current
    job record and should return the new destruction time. Otherwise, any
    destruction time before the configured maximum lifetime will be allowed.
    """

    validate_execution_duration: ExecutionDurationValidator | None = None
    """Validate a new execution duration for a job.

    If provided, called with the requested execution duration and the current
    job record and should return the new execution duration time. Otherwise,
    any execution duration time shorter than the configured maximum timeout
    will be allowed.
    """

    wait_timeout: timedelta = timedelta(minutes=1)
    """Maximum time a client can wait for a job change."""
