"""Support functions for testing UWS code."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from dramatiq.brokers.stub import StubBroker
from dramatiq.middleware import CurrentMessage

from vocutouts.uws.config import UWSConfig
from vocutouts.uws.models import ExecutionPhase

if TYPE_CHECKING:
    from pathlib import Path

    from vocutouts.uws.models import Job
    from vocutouts.uws.service import JobService

uws_broker = StubBroker()
"""Dramatiq broker for use in tests."""

uws_broker.add_middleware(CurrentMessage())


def build_uws_config(tmp_path: Path) -> UWSConfig:
    """Set up a test configuration.

    This currently requires the database URL and Redis hostname be set in the
    environment, which is done as part of running the test with tox-docker.
    """
    return UWSConfig(
        execution_duration=10 * 60,
        lifetime=24 * 60 * 60,
        database_url=os.environ["CUTOUT_DATABASE_URL"],
        database_password=os.getenv("CUTOUT_DATABASE_PASSWORD"),
        redis_host=os.getenv("CUTOUT_REDIS_HOST", "127.0.0.1"),
        redis_password=None,
    )


async def wait_for_job(job_service: JobService, user: str, job_id: str) -> Job:
    """Wait for a job that was just started and return it."""
    job = await job_service.get(
        "user", "1", wait=5, wait_phase=ExecutionPhase.QUEUED
    )
    while job.phase in (ExecutionPhase.QUEUED, ExecutionPhase.EXECUTING):
        job = await job_service.get("user", "1", wait=5, wait_phase=job.phase)
    return job
