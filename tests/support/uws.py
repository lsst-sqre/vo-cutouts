"""Support functions for testing UWS code."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, Mock, patch

import dramatiq
import structlog
from dramatiq import Actor, Message
from dramatiq.brokers.stub import StubBroker
from dramatiq.middleware import CurrentMessage, Middleware
from dramatiq.results import Results
from dramatiq.results.backends import StubBackend
from google.cloud import storage
from lsst.daf.butler import Butler, ButlerURI

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
            "collection": "output/collection",
            "data_id": {"visit": 903332, "detector": 20, "instrument": "HSC"},
            "datatype": "calexp_cutouts",
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
        butler_repository="some-imaginary-repository-url",
        execution_duration=10 * 60,
        lifetime=24 * 60 * 60,
        database_url=os.environ["CUTOUT_DATABASE_URL"],
        database_password=os.getenv("CUTOUT_DATABASE_PASSWORD"),
        redis_host=os.getenv("CUTOUT_REDIS_HOST", "127.0.0.1"),
        redis_password=None,
    )


def _mock_butler_getURI(
    datatype: str, *, dataId: Dict[str, str], collections: List[str]
) -> ButlerURI:
    assert datatype == "calexp_cutouts"
    assert dataId == {"visit": 903332, "detector": 20, "instrument": "HSC"}
    assert collections == ["output/collection"]
    mock = Mock(spec=ButlerURI)
    mock.scheme = "s3"
    mock.netloc = "some-bucket"
    mock.path = "/some/path"
    mock.relativeToPathRoot = "some/path"
    return mock


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


def mock_uws_butler() -> Iterator[None]:
    with patch("vocutouts.uws.results.Butler") as m1:
        m1.return_value = Mock(spec=Butler)
        m1.return_value.registry = MagicMock()
        m1.return_value.getURI.side_effect = _mock_butler_getURI
        mock_gcs = MockStorageClient
        with patch("google.cloud.storage.Client", side_effect=mock_gcs):
            yield


async def wait_for_job(job_service: JobService, user: str, job_id: str) -> Job:
    """Wait for a job that was just started and return it."""
    job = await job_service.get(
        "user", "1", wait=5, wait_phase=ExecutionPhase.QUEUED
    )
    while job.phase in (ExecutionPhase.QUEUED, ExecutionPhase.EXECUTING):
        job = await job_service.get("user", "1", wait=5, wait_phase=job.phase)
    return job
