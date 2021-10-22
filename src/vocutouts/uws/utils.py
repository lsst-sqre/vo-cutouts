"""Utility functions for services using UWS."""

from __future__ import annotations

from datetime import timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import datetime


def isodatetime(timestamp: datetime) -> str:
    """Format a timestamp in UTC in the expected UWS ISO date format."""
    assert timestamp.tzinfo in (None, timezone.utc)
    return timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
