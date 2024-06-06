"""Constants for the UWS work queue."""

from __future__ import annotations

from datetime import timedelta

__all__ = [
    "JOB_RESULT_TIMEOUT",
    "UWS_QUEUE_NAME",
]

JOB_RESULT_TIMEOUT = timedelta(seconds=5)
"""How long to poll arq for job results before giving up."""

UWS_QUEUE_NAME = "uws:queue"
"""Name of the arq queue for internal UWS messages."""
