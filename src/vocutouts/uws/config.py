"""Configuration for the UWS service."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Optional

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

    butler_repository: str
    """The Butler repository to query for results."""

    execution_duration: int
    """Maximum execution time in seconds.

    Jobs that run longer than this length of time will be automatically
    aborted.
    """

    lifetime: int
    """The lifetime of jobs in seconds.

    After this much time elapses since the creation of the job, all of the
    results from the job will be cleaned up and all record of the job will be
    deleted.
    """

    database_url: str
    """URL for the metadata database."""

    redis_host: str
    """Hostname of the Redis server used by Dramatiq."""

    database_password: Optional[str] = None
    """Password for the database."""

    redis_password: Optional[str] = None
    """Password for the Redis server used by Dramatiq."""

    wait_timeout: int = 60
    """Maximum time in seconds a client can wait for a job change."""
