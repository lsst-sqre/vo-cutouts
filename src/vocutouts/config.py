"""Configuration definition."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field, SecretStr
from pydantic_settings import SettingsConfigDict
from safir.logging import LogLevel, Profile
from safir.uws import UWSApplication, UWSAppSettings, UWSConfig, UWSRoute
from vo_models.uws import JobSummary

from .dependencies import get_params_dependency, post_params_dependency
from .models.cutout import CutoutParameters, CutoutXmlParameters

__all__ = [
    "Config",
    "config",
    "uws",
]


class Config(UWSAppSettings):
    """Configuration for vo-cutouts."""

    slack_webhook: SecretStr | None = Field(
        None,
        title="Slack webhook for alerts",
        description="If set, alerts will be posted to this Slack webhook",
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
    def uws_config(self) -> UWSConfig:
        """Corresponding configuration for the UWS subsystem."""
        return self.build_uws_config(
            job_summary_type=JobSummary[CutoutXmlParameters],
            parameters_type=CutoutParameters,
            slack_webhook=self.slack_webhook,
            worker="cutout",
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
