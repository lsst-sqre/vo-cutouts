"""Configuration definition."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

from arq.connections import RedisSettings
from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from safir.arq import ArqMode, build_arq_redis_settings
from safir.logging import LogLevel, Profile
from safir.pydantic import (
    EnvAsyncPostgresDsn,
    EnvRedisDsn,
    HumanTimedelta,
    SecondsTimedelta,
)
from safir.uws import UWSApplication, UWSConfig, UWSRoute

from .dependencies import get_params_dependency, post_params_dependency
from .models.cutout import CutoutParameters

__all__ = [
    "Config",
    "config",
    "uws",
]


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
        return build_arq_redis_settings(
            self.arq_queue_url, self.arq_queue_password
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
