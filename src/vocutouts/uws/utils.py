"""Utility functions for services using UWS."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Optional


def isodatetime(timestamp: datetime) -> str:
    """Format a timestamp in UTC in the expected UWS ISO date format."""
    assert timestamp.tzinfo in (None, timezone.utc)
    return timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_isodatetime(time_string: str) -> Optional[datetime]:
    """Parse a string in the UWS ISO date format.

    Returns
    -------
    timestamp : `datetime.datetime` or `None`
        The corresponding `datetime.datetime` or `None` if the string is
        invalid.
    """
    if not time_string.endswith("Z"):
        return None
    try:
        return datetime.fromisoformat(time_string[:-1] + "+00:00")
    except Exception:
        return None
