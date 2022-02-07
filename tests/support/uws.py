"""Support functions for testing UWS code."""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING
from unittest.mock import Mock, patch

import dramatiq
import structlog
from dramatiq import Actor, Message
from dramatiq.brokers.stub import StubBroker
from dramatiq.middleware import CurrentMessage, Middleware
from dramatiq.results import Results
from dramatiq.results.backends import StubBackend
from google.cloud import storage

from vocutouts.uws.config import UWSConfig
from vocutouts.uws.database import create_sync_session
from vocutouts.uws.jobs import (
    uws_job_completed,
    uws_job_failed,
    uws_job_started,
)
from vocutouts.uws.models import ExecutionPhase
from vocutouts.uws.policy import UWSPolicy
from vocutouts.uws.utils import isodatetime, parse_isodatetime

if TYPE_CHECKING:
    from pathlib import Path
    from typing import Any, Dict, Iterator, List, Optional

    from dramatiq import Broker, Worker
    from sqlalchemy.orm import scoped_session

    from vocutouts.uws.models import Job, JobParameter
    from vocutouts.uws.service import JobService

SOURCE_UUID = str(uuid.uuid4())
"""UUID of the source for a cutout."""

uws_broker = StubBroker()
"""Dramatiq broker for use in tests."""

results = StubBackend()
"""Result backend used by UWS."""

worker_session: Optional[scoped_session] = None
"""Shared scoped session used by the UWS worker."""

uws_broker.add_middleware(CurrentMessage())
uws_broker.add_middleware(Results(backend=results))


class WorkerSession(Middleware):
    """Middleware to create a SQLAlchemy scoped session for a worker."""

    def __init__(self, config: UWSConfig) -> None:
        self._config = config

    def before_worker_boot(self, broker: Broker, worker: Worker) -> None:
        """Initialize the database session before worker threads start.

        This is run in the main process by the ``dramatiq`` CLI before
        starting the worker threads, so it should run in a single-threaded
        context.
        """
        logger = structlog.get_logger("uws")
        global worker_session
        if worker_session is None:
            worker_session = create_sync_session(self._config, logger)


@dramatiq.actor(broker=uws_broker, queue_name="job", store_results=True)
def trivial_job(job_id: str) -> List[Dict[str, Any]]:
    message = CurrentMessage.get_current_message()
    now = datetime.now(tz=timezone.utc)
    job_started.send(job_id, message.message_id, isodatetime(now))
    return [
        {
            "result_id": "cutout",
            "url": "s3://some-bucket/some/path",
            "mime_type": "application/fits",
        }
    ]


@dramatiq.actor(broker=uws_broker, queue_name="uws", priority=0)
def job_started(job_id: str, message_id: str, start_time: str) -> None:
    logger = structlog.get_logger("uws")
    start = parse_isodatetime(start_time)
    assert worker_session
    assert start, f"Invalid start timestamp {start_time}"
    uws_job_started(job_id, message_id, start, worker_session, logger)


@dramatiq.actor(broker=uws_broker, queue_name="uws", priority=10)
def job_completed(
    message: Dict[str, Any], result: List[Dict[str, str]]
) -> None:
    logger = structlog.get_logger("uws")
    job_id = message["args"][0]
    assert worker_session
    uws_job_completed(job_id, result, worker_session, logger)


@dramatiq.actor(broker=uws_broker, queue_name="uws", priority=20)
def job_failed(message: Dict[str, Any], exception: Dict[str, str]) -> None:
    logger = structlog.get_logger("uws")
    job_id = message["args"][0]
    assert worker_session
    uws_job_failed(job_id, exception, worker_session, logger)


class TrivialPolicy(UWSPolicy):
    def __init__(self, actor: Actor) -> None:
        super().__init__()
        self.actor = actor

    def dispatch(self, job: Job) -> Message:
        return self.actor.send_with_options(
            args=(job.job_id,),
            on_success=job_completed,
            on_failure=job_failed,
        )

    def validate_destruction(
        self, destruction: datetime, job: Job
    ) -> datetime:
        return destruction

    def validate_execution_duration(
        self, execution_duration: int, job: Job
    ) -> int:
        return execution_duration

    def validate_params(self, params: List[JobParameter]) -> None:
        pass


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


class MockBlob(Mock):
    def __init__(self) -> None:
        super().__init__(spec=storage.blob.Blob)

    def generate_signed_url(
        self,
        *,
        version: str,
        expiration: timedelta,
        method: str,
        response_type: str,
        credentials: Any,
    ) -> str:
        assert version == "v4"
        assert expiration == timedelta(seconds=15 * 60)
        assert method == "GET"
        assert response_type == "application/fits"
        return "https://example.com/cutout-result"


class MockBucket(Mock):
    def __init__(self) -> None:
        super().__init__(spec=storage.bucket.Bucket)

    def blob(self, blob_name: str) -> Mock:
        assert blob_name == "some/path"
        return MockBlob()


class MockStorageClient(Mock):
    def __init__(self) -> None:
        super().__init__(spec=storage.Client)

    def bucket(self, bucket_name: str) -> Mock:
        assert bucket_name == "some-bucket"
        return MockBucket()


def mock_uws_google_storage() -> Iterator[None]:
    mock_gcs = MockStorageClient
    with patch("google.cloud.storage.Client", side_effect=mock_gcs):
        with patch("google.auth.compute_engine.IDTokenCredentials"):
            yield


async def wait_for_job(job_service: JobService, user: str, job_id: str) -> Job:
    """Wait for a job that was just started and return it."""
    job = await job_service.get(
        "user", "1", wait=5, wait_phase=ExecutionPhase.QUEUED
    )
    while job.phase in (ExecutionPhase.QUEUED, ExecutionPhase.EXECUTING):
        job = await job_service.get("user", "1", wait=5, wait_phase=job.phase)

    # Despite prioritization of messages, there can still be a race condition
    # where the completion message is processed before the start message, so
    # the job is seen as complete but the start time is not populated.  Wait
    # for the start time to show up as well.
    count = 0
    while job.start_time is None and count < 10:
        await asyncio.sleep(0.5)
        count += 1
        job = await job_service.get("user", "1")

    return job
