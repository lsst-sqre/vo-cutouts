"""Tests for arq worker construction."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import ANY

import pytest
from arq.constants import default_queue_name
from arq.jobs import JobStatus
from arq.worker import Function
from safir.arq import JobMetadata, MockArqQueue
from safir.datetime import current_datetime
from structlog.stdlib import BoundLogger

from vocutouts.uws.config import UWSConfig
from vocutouts.uws.constants import UWS_QUEUE_NAME
from vocutouts.uws.dependencies import UWSFactory
from vocutouts.uws.exceptions import TaskFatalError
from vocutouts.uws.models import (
    ErrorCode,
    ErrorType,
    ExecutionPhase,
    UWSJobResult,
)
from vocutouts.uws.storage import JobStore
from vocutouts.uws.workers import build_uws_worker, build_worker


@pytest.mark.asyncio
async def test_build_worker(
    uws_config: UWSConfig, logger: BoundLogger
) -> None:
    def worker(
        job_id: str, name: str, *, greeting: str, logger: BoundLogger
    ) -> list[UWSJobResult]:
        return [
            UWSJobResult(
                result_id="greeting",
                url=f"https://example.com/{greeting}/{name}",
            )
        ]

    # Construct the arq configuration and check it.
    settings = build_worker(worker, uws_config, logger)
    assert len(settings.functions) == 1
    assert isinstance(settings.functions[0], Function)
    assert settings.functions[0].name == worker.__qualname__
    assert settings.redis_settings == uws_config.arq_redis_settings
    assert settings.queue_name == default_queue_name
    assert settings.allow_abort_jobs
    assert settings.on_startup
    assert settings.on_shutdown

    # Run the startup hook.
    ctx: dict[Any, Any] = {}
    startup = settings.on_startup
    await startup(ctx)
    assert isinstance(ctx["arq"], MockArqQueue)
    assert isinstance(ctx["logger"], BoundLogger)
    arq = ctx["arq"]
    assert arq.default_queue_name == UWS_QUEUE_NAME

    # Run the worker.
    function = settings.functions[0].coroutine
    result = await function(ctx, "42", "Roger", greeting="Hello")
    assert result == [
        UWSJobResult(
            result_id="greeting", url="https://example.com/Hello/Roger"
        )
    ]
    assert list(arq._job_metadata[UWS_QUEUE_NAME].values()) == [
        JobMetadata(
            id=ANY,
            name="job_started",
            args=("42", ANY),
            kwargs={},
            enqueue_time=ANY,
            status=JobStatus.queued,
            queue_name=UWS_QUEUE_NAME,
        ),
        JobMetadata(
            id=ANY,
            name="job_completed",
            args=("42",),
            kwargs={},
            enqueue_time=ANY,
            status=JobStatus.queued,
            queue_name=UWS_QUEUE_NAME,
        ),
    ]

    # Run the shutdown hook.
    shutdown = settings.on_shutdown
    await shutdown(ctx)


@pytest.mark.asyncio
async def test_build_uws_worker(
    arq_queue: MockArqQueue,
    uws_config: UWSConfig,
    uws_factory: UWSFactory,
    logger: BoundLogger,
) -> None:
    settings = build_uws_worker(uws_config, logger)
    job_service = uws_factory.create_job_service()
    job = await job_service.create("user", params=[])
    results = [UWSJobResult(result_id="greeting", url="https://example.com")]
    await job_service.start("user", job.job_id, "some-token")
    job = await job_service.get("user", job.job_id)
    assert job.start_time is None
    assert job.phase == ExecutionPhase.QUEUED

    # Construct the arq configuration and check it.
    settings = build_uws_worker(uws_config, logger)
    assert len(settings.functions) == 2
    job_started = settings.functions[0]
    assert callable(job_started)
    job_completed = settings.functions[1]
    assert callable(job_completed)
    assert settings.redis_settings == uws_config.arq_redis_settings
    assert settings.queue_name == UWS_QUEUE_NAME
    assert not settings.allow_abort_jobs
    assert settings.on_startup
    assert settings.on_shutdown

    # Run the startup hook.
    ctx: dict[Any, Any] = {}
    startup = settings.on_startup
    await startup(ctx)
    assert isinstance(ctx["arq"], MockArqQueue)
    assert isinstance(ctx["logger"], BoundLogger)
    assert isinstance(ctx["storage"], JobStore)
    assert ctx["arq"].default_queue_name == default_queue_name
    ctx["arq"] = arq_queue

    # Test starting a job.
    now = current_datetime()
    assert job.message_id
    await arq_queue.set_in_progress(job.message_id)
    await job_started(ctx, job.job_id, now)
    job = await job_service.get("user", job.job_id)
    assert job.phase == ExecutionPhase.EXECUTING
    assert job.start_time == now

    # Test finishing a job.
    assert job.message_id
    await asyncio.gather(
        job_completed(ctx, job.job_id),
        arq_queue.set_complete(job.message_id, result=results),
    )
    job = await job_service.get("user", job.job_id)
    assert job.phase == ExecutionPhase.COMPLETED
    assert job.end_time
    assert job.end_time.microsecond == 0
    assert now <= job.end_time <= current_datetime()
    assert job.results == results

    # Test starting and erroring a job with a TaskError.
    job = await job_service.create("user", params=[])
    await job_service.start("user", job.job_id, "some-token")
    job = await job_service.get("user", job.job_id)
    assert job.message_id
    await arq_queue.set_in_progress(job.message_id)
    await job_started(ctx, job.job_id, now)
    error = TaskFatalError(ErrorCode.ERROR, "Something", "went wrong")
    await asyncio.gather(
        job_completed(ctx, job.job_id),
        arq_queue.set_complete(job.message_id, result=error, success=False),
    )
    job = await job_service.get("user", job.job_id)
    assert job.phase == ExecutionPhase.ERROR
    assert job.end_time
    assert job.end_time.microsecond == 0
    assert now <= job.end_time <= current_datetime()
    assert job.error
    assert job.error.error_type == ErrorType.FATAL
    assert job.error.error_code == ErrorCode.ERROR
    assert job.error.message == "Something"
    assert job.error.detail == "went wrong"

    # Test starting and erroring a job with an unknown exception.
    job = await job_service.create("user", params=[])
    await job_service.start("user", job.job_id, "some-token")
    job = await job_service.get("user", job.job_id)
    assert job.message_id
    await arq_queue.set_in_progress(job.message_id)
    await job_started(ctx, job.job_id, now)
    exc = ValueError("some error")
    await asyncio.gather(
        job_completed(ctx, job.job_id),
        arq_queue.set_complete(job.message_id, result=exc, success=False),
    )
    job = await job_service.get("user", job.job_id)
    assert job.phase == ExecutionPhase.ERROR
    assert job.error
    assert job.error.error_type == ErrorType.TRANSIENT
    assert job.error.error_code == ErrorCode.ERROR
    assert job.error.message == "Unknown error executing task"
    assert job.error.detail == "ValueError: some error"
