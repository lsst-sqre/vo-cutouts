"""Worker for UWS database updates."""

from __future__ import annotations

import structlog
from safir.logging import configure_logging
from safir.sentry import initialize_sentry

from .. import __version__
from ..config import config, uws

__all__ = ["WorkerSettings"]


initialize_sentry(release=__version__)

configure_logging(
    name="vocutouts", profile=config.log_profile, log_level=config.log_level
)

WorkerSettings = uws.build_worker(structlog.get_logger("vocutouts"))
"""arq configuration for the UWS database worker."""
