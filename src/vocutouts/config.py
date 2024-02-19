"""Configuration definition."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

from pydantic import Field, PostgresDsn, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from safir.logging import LogLevel, Profile

from .uws.config import UWSConfig

__all__ = ["Config", "config"]


class Config(BaseSettings):
    """Configuration for vo-cutouts."""

    database_url: PostgresDsn = Field(..., title="URL for UWS job database")

    database_password: str | None = Field(
        None, title="Password for UWS job database"
    )

    lifetime: timedelta = Field(
        timedelta(days=7), title="Lifetime of cutout job results"
    )

    redis_host: str = Field(
        ...,
        title="Hostname of Redis server",
        description=(
            "The Redis server is used as the backing store for the Dramatiq"
            " work queue"
        ),
    )

    redis_password: str | None = Field(
        None,
        title="Password for Redis server",
        description=(
            "The Redis server is used as the backing store for the Dramatiq"
            " work queue"
        ),
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

    storage_url: str = Field(
        ...,
        title="Root URL for cutout results",
        description=(
            "Must be an ``s3`` URL pointing to a Google Cloud Storage bucket"
            " that is writable by the backend and readable by the frontend."
        ),
    )

    sync_timeout: timedelta = Field(
        timedelta(minutes=1), title="Timeout for sync requests"
    )

    timeout: timedelta = Field(
        timedelta(minutes=10), title="Timeout for cutout jobs"
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

    @field_validator("lifetime", "sync_timeout", "timeout", mode="before")
    @classmethod
    def _parse_as_seconds(cls, v: int | str | timedelta) -> int | timedelta:
        """Convert timedelta strings so they are parsed as seconds."""
        if isinstance(v, timedelta):
            return v
        try:
            return int(v)
        except ValueError as e:
            msg = f"value {v} must be an integer number of seconds"
            raise ValueError(msg) from e

    def uws_config(self) -> UWSConfig:
        """Convert to configuration for the UWS subsystem."""
        return UWSConfig(
            execution_duration=self.timeout,
            lifetime=self.lifetime,
            database_url=str(self.database_url),
            database_password=self.database_password,
            redis_host=self.redis_host,
            redis_password=self.redis_password,
            signing_service_account=self.service_account,
        )


config = Config()
"""Configuration for vo-cutouts."""
