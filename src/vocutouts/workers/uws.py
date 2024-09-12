"""Worker for UWS database updates."""

from __future__ import annotations

import structlog
from safir.logging import configure_logging

from ..config import config, uws

__all__ = ["WorkerSettings"]


configure_logging(
    name="vocutouts", profile=config.profile, log_level=config.log_level
)

WorkerSettings = uws.build_worker(
    structlog.get_logger("vocutouts"), check_schema=True
)
"""arq configuration for the UWS database worker."""
