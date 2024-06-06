"""Worker for UWS database updates."""

from __future__ import annotations

import structlog
from safir.logging import configure_logging

from ..config import config
from ..uws.workers import build_uws_worker

__all__ = ["WorkerSettings"]


configure_logging(
    name="vocutouts", profile=config.profile, log_level=config.log_level
)

WorkerSettings = build_uws_worker(
    config.uws_config, structlog.get_logger("vocutouts")
)
"""arq configuration for the UWS database worker."""
