"""All database schema objects."""

from __future__ import annotations

from typing import TYPE_CHECKING

from .base import Base
from .job import Job
from .job_parameter import JobParameter
from .job_result import JobResult

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

__all__ = [
    "Job",
    "JobParameter",
    "JobResult",
    "drop_schema",
    "initialize_schema",
]


async def drop_schema(engine: AsyncEngine) -> None:
    """Drop all tables to reset the database."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


async def initialize_schema(engine: AsyncEngine) -> None:
    """Initialize the database with all schema."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
