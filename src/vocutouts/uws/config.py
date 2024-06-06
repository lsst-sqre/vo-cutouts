"""Configuration for the UWS service."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from arq.connections import RedisSettings
from pydantic import SecretStr
from safir.arq import ArqMode

__all__ = ["UWSConfig"]


@dataclass
class UWSConfig:
    """Configuration for the UWS service.

    The UWS service may be embedded in a variety of VO applications.  This
    class encapsulates the configuration of the UWS component that may vary by
    service or specific installation.  The calling application with a UWS
    component should create this config object with appropriate settings from
    whatever source and then pass that into the
    `vocutouts.uws.dependencies.UWSDependency` object.
    """

    execution_duration: timedelta
    """Maximum execution time in seconds.

    Jobs that run longer than this length of time will be automatically
    aborted.
    """

    lifetime: timedelta
    """The lifetime of jobs in seconds.

    After this much time elapses since the creation of the job, all of the
    results from the job will be cleaned up and all record of the job will be
    deleted.
    """

    database_url: str
    """URL for the metadata database."""

    arq_mode: ArqMode
    """What mode to use for the arq queue."""

    arq_redis_settings: RedisSettings
    """Settings for Redis for the arq queue."""

    signing_service_account: str
    """Email of service account to use for signed URLs.

    The default credentials that the application frontend runs with must have
    the ``roles/iam.serviceAccountTokenCreator`` role on the service account
    with this email.
    """

    database_password: SecretStr | None = None
    """Password for the database."""

    url_lifetime: timedelta = timedelta(minutes=15)
    """How long result URLs should be valid for."""

    wait_timeout: timedelta = timedelta(minutes=1)
    """Maximum time a client can wait for a job change."""
