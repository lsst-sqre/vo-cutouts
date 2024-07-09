"""Constants for the UWS work queue."""

from __future__ import annotations

from datetime import timedelta

from arq.cron import Options

__all__ = [
    "JOB_RESULT_TIMEOUT",
    "UWS_DATABASE_TIMEOUT",
    "UWS_EXPIRE_JOBS_SCHEDULE",
    "UWS_QUEUE_NAME",
]

JOB_RESULT_TIMEOUT = timedelta(seconds=5)
"""How long to poll arq for job results before giving up."""

UWS_DATABASE_TIMEOUT = timedelta(seconds=30)
"""Timeout on workers that update the UWS database."""

UWS_EXPIRE_JOBS_SCHEDULE = Options(
    month=None,
    day=None,
    weekday=None,
    hour=None,
    minute=5,
    second=0,
    microsecond=0,
)
"""Schedule for job expiration cron job, as `arq.cron.cron` parameters."""

UWS_QUEUE_NAME = "uws:queue"
"""Name of the arq queue for internal UWS messages."""
