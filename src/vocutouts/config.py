"""Configuration definition."""

from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path
from typing import Annotated, TypeAlias

from arq.connections import RedisSettings
from pydantic import (
    AfterValidator,
    BeforeValidator,
    Field,
    RedisDsn,
    SecretStr,
    UrlConstraints,
)
from pydantic_core import MultiHostUrl, Url
from pydantic_settings import BaseSettings, SettingsConfigDict
from safir.arq import ArqMode
from safir.datetime import parse_timedelta
from safir.logging import LogLevel, Profile

from .dependencies import get_params_dependency, post_params_dependency
from .models.cutout import CutoutParameters
from .uws.app import UWSApplication
from .uws.config import UWSConfig, UWSRoute

__all__ = [
    "Config",
    "EnvAsyncPostgresDsn",
    "HumanTimedelta",
    "SecondsTimedelta",
    "config",
    "uws",
]

"""PostgreSQL data source URL using either ``asyncpg`` or no driver."""


def _validate_env_async_postgres_dsn(v: MultiHostUrl) -> MultiHostUrl:
    """Possibly adjust a PostgreSQL DSN based on environment variables.

    When run via tox and tox-docker, the PostgreSQL hostname and port will be
    randomly selected and exposed only in environment variables. We have to
    patch that into the database URL at runtime since `tox doesn't have a way
    of substituting it into the environment
    <https://github.com/tox-dev/tox-docker/issues/55>`__.
    """
    if port := os.getenv("POSTGRES_5432_TCP_PORT"):
        old_host = v.hosts()[0]
        return MultiHostUrl.build(
            scheme=v.scheme,
            username=old_host.get("username"),
            password=old_host.get("password"),
            host=os.getenv("POSTGRES_HOST", old_host.get("host")),
            port=int(port),
            path=v.path.lstrip("/") if v.path else v.path,
            query=v.query,
            fragment=v.fragment,
        )
    else:
        return v


EnvAsyncPostgresDsn: TypeAlias = Annotated[
    MultiHostUrl,
    UrlConstraints(
        host_required=True,
        allowed_schemes=["postgresql", "postgresql+asyncpg"],
    ),
    AfterValidator(_validate_env_async_postgres_dsn),
]
"""Async PostgreSQL data source URL honoring Docker environment variables."""


def _validate_env_redis_dsn(v: RedisDsn) -> RedisDsn:
    """Possibly adjust a Redis DSN based on environment variables.

    When run via tox and tox-docker, the Redis hostname and port will be
    randomly selected and exposed only in environment variables. We have to
    patch that into the Redis URL at runtime since `tox doesn't have a way of
    substituting it into the environment
    <https://github.com/tox-dev/tox-docker/issues/55>`__.
    """
    if port := os.getenv("REDIS_6379_TCP_PORT"):
        return RedisDsn.build(
            scheme=v.scheme,
            username=v.username,
            password=v.password,
            host=os.getenv("REDIS_HOST", v.unicode_host() or "localhost"),
            port=int(port),
            path=v.path.lstrip("/") if v.path else v.path,
            query=v.query,
            fragment=v.fragment,
        )
    else:
        return v


EnvRedisDsn: TypeAlias = Annotated[
    Url,
    UrlConstraints(host_required=True, allowed_schemes=["redis"]),
    AfterValidator(_validate_env_redis_dsn),
]
"""Redis data source URL honoring Docker environment variables."""


def _parse_timedelta(v: str | float | timedelta) -> float | timedelta:
    if not isinstance(v, str):
        return v
    try:
        return int(v)
    except ValueError:
        return parse_timedelta(v)


HumanTimedelta: TypeAlias = Annotated[
    timedelta, BeforeValidator(_parse_timedelta)
]
"""Parse a human-readable string into a `datetime.timedelta`.

Accepts as input an integer (or stringified integer) number of seconds, an
already-parsed `~datetime.timedelta`, or a string consisting of one or more
sequences of numbers and duration abbreviations, separated by optional
whitespace.  Whitespace at the beginning and end of the string is ignored. The
supported abbreviations are:

- Week: ``weeks``, ``week``, ``w``
- Day: ``days``, ``day``, ``d``
- Hour: ``hours``, ``hour``, ``hr``, ``h``
- Minute: ``minutes``, ``minute``, ``mins``, ``min``, ``m``
- Second: ``seconds``, ``second``, ``secs``, ``sec``, ``s``

If several are present, they must be given in the above order. Example
valid strings are ``8d`` (8 days), ``4h 3minutes`` (four hours and three
minutes), and ``5w4d`` (five weeks and four days).
"""

SecondsTimedelta: TypeAlias = Annotated[
    timedelta,
    BeforeValidator(lambda v: v if not isinstance(v, str) else int(v)),
]
"""Parse an integer number of seconds into a `datetime.timedelta`.

Accepts as input an integer (or stringified integer) number of seconds or an
already-parsed `~datetime.timedelta`. Compared to the built-in Pydantic
handling of `~datetime.timedelta`, an integer number of seconds as a string is
accepted, and ISO 8601 durations are not supported.
"""


class Config(BaseSettings):
    """Configuration for vo-cutouts."""

    arq_mode: ArqMode = Field(
        ArqMode.production,
        title="arq operation mode",
        description="This will always be production outside the test suite",
    )

    arq_queue_url: EnvRedisDsn = Field(
        ...,
        title="arq Redis DSN",
        description="DSN of Redis server to use for the arq queue",
    )

    arq_queue_password: SecretStr | None = Field(
        None,
        title="Password for arq Redis server",
        description="Password of Redis server to use for the arq queue",
    )

    database_url: EnvAsyncPostgresDsn = Field(
        ...,
        title="PostgreSQL DSN",
        description="DSN of PostgreSQL database for UWS job tracking",
    )

    database_password: SecretStr | None = Field(
        None, title="Password for UWS job database"
    )

    grace_period: SecondsTimedelta = Field(
        timedelta(seconds=30),
        title="Grace period for jobs",
        description=(
            "How long to wait for a job to finish on shutdown before"
            " canceling it"
        ),
    )

    lifetime: HumanTimedelta = Field(
        timedelta(days=7), title="Lifetime of cutout job results"
    )

    service_account: str = Field(
        ...,
        title="Service account for URL signing",
        description=(
            "Email of the service account to use for signed URLs of results."
            " The default credentials that the application frontend runs with"
            " must have the ``roles/iam.serviceAccountTokenCreator`` role on"
            " the service account with this email."
        ),
    )

    slack_webhook: SecretStr | None = Field(
        None,
        title="Slack webhook for alerts",
        description="If set, alerts will be posted to this Slack webhook",
    )

    storage_url: str = Field(
        ...,
        title="Root URL for cutout results",
        description=(
            "Must be a ``gs`` or ``s3`` URL pointing to a Google Cloud Storage"
            " bucket that is writable by the backend and readable by the"
            " frontend."
        ),
    )

    sync_timeout: HumanTimedelta = Field(
        timedelta(minutes=1), title="Timeout for sync requests"
    )

    timeout: SecondsTimedelta = Field(
        timedelta(minutes=10),
        title="Cutout job timeout in seconds",
        description=(
            "Must be given as a number of seconds as a string or integer"
        ),
    )

    tmpdir: Path = Field(Path("/tmp"), title="Temporary directory for workers")

    name: str = Field("vo-cutouts", title="Name of application")

    path_prefix: str = Field("/api/cutout", title="URL prefix for application")

    profile: Profile = Field(
        Profile.development, title="Application logging profile"
    )

    log_level: LogLevel = Field(
        LogLevel.INFO, title="Log level of the application's logger"
    )

    model_config = SettingsConfigDict(
        env_prefix="CUTOUT_", case_sensitive=False
    )

    @property
    def arq_redis_settings(self) -> RedisSettings:
        """Redis settings for arq."""
        database = 0
        if self.arq_queue_url.path:
            database = int(self.arq_queue_url.path.lstrip("/"))
        if self.arq_queue_password:
            password = self.arq_queue_password.get_secret_value()
        else:
            password = None
        return RedisSettings(
            host=self.arq_queue_url.unicode_host() or "localhost",
            port=self.arq_queue_url.port or 6379,
            database=database,
            password=password,
        )

    @property
    def uws_config(self) -> UWSConfig:
        """Corresponding configuration for the UWS subsystem."""
        return UWSConfig(
            arq_mode=self.arq_mode,
            arq_redis_settings=self.arq_redis_settings,
            execution_duration=self.timeout,
            lifetime=self.lifetime,
            parameters_type=CutoutParameters,
            signing_service_account=self.service_account,
            worker="cutout",
            database_url=str(self.database_url),
            database_password=self.database_password,
            slack_webhook=self.slack_webhook,
            sync_timeout=self.sync_timeout,
            async_post_route=UWSRoute(
                dependency=post_params_dependency,
                summary="Create async cutout job",
                description="Create a new UWS job to perform an image cutout",
            ),
            sync_get_route=UWSRoute(
                dependency=get_params_dependency,
                summary="Synchronous cutout",
                description=(
                    "Synchronously request a cutout. This will wait for the"
                    " cutout to be completed and return the resulting image"
                    " as a FITS file. The image will be returned via a"
                    " redirect to a URL at the underlying object store."
                ),
            ),
            sync_post_route=UWSRoute(
                dependency=post_params_dependency,
                summary="Synchronous cutout",
                description=(
                    "Synchronously request a cutout. This will wait for the"
                    " cutout to be completed and return the resulting image"
                    " as a FITS file. The image will be returned via a"
                    " redirect to a URL at the underlying object store."
                ),
            ),
        )


config = Config()
"""Configuration for vo-cutouts."""

uws = UWSApplication(config.uws_config)
"""The UWS application for this service."""
