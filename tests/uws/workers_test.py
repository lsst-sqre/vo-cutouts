"""Tests for arq worker construction."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any
from unittest.mock import ANY

import pytest
from arq.constants import default_queue_name
from arq.jobs import JobStatus
from arq.worker import Function
from safir.arq import JobMetadata, MockArqQueue
from safir.datetime import current_datetime
from safir.testing.slack import MockSlackWebhook
from structlog.stdlib import BoundLogger

from vocutouts.uws.app import UWSApplication
from vocutouts.uws.config import UWSConfig
from vocutouts.uws.constants import UWS_QUEUE_NAME
from vocutouts.uws.dependencies import UWSFactory
from vocutouts.uws.models import (
    ErrorCode,
    ErrorType,
    ExecutionPhase,
    UWSJobParameter,
    UWSJobResult,
)
from vocutouts.uws.storage import JobStore
from vocutouts.uws.uwsworker import (
    WorkerConfig,
    WorkerFatalError,
    WorkerJobInfo,
    WorkerResult,
    build_worker,
)

from ..support.uws import SimpleParameters


@pytest.mark.asyncio
async def test_build_worker(
    uws_config: UWSConfig, logger: BoundLogger
) -> None:
    def hello(
        params: SimpleParameters, info: WorkerJobInfo, logger: BoundLogger
    ) -> list[WorkerResult]:
        return [
            WorkerResult(
                result_id="greeting", url=f"https://example.com/{params.name}"
            )
        ]

    # Construct the arq configuration and check it.
    redis_settings = uws_config.arq_redis_settings
    worker_config = WorkerConfig(
        arq_mode=uws_config.arq_mode,
        arq_queue_url=(
            f"redis://{redis_settings.host}:{redis_settings.port}"
            f"/{redis_settings.database}"
        ),
        arq_queue_password=redis_settings.password,
        timeout=uws_config.execution_duration,
    )
    settings = build_worker(hello, worker_config, logger)
    assert len(settings.functions) == 1
    assert isinstance(settings.functions[0], Function)
    assert settings.functions[0].name == hello.__qualname__
    assert settings.redis_settings == uws_config.arq_redis_settings
    assert settings.queue_name == default_queue_name
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
    params = SimpleParameters(name="Roger")
    info = WorkerJobInfo(
        job_id="42",
        user="someuser",
        token="some-token",
        timeout=timedelta(minutes=1),
        run_id="some-run-id",
    )
    result = await function(ctx, params, info)
    assert result == [
        WorkerResult(result_id="greeting", url="https://example.com/Roger")
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
    mock_slack: MockSlackWebhook,
    logger: BoundLogger,
) -> None:
    uws = UWSApplication(uws_config)
    job_service = uws_factory.create_job_service()
    job = await job_service.create(
        "user", params=[UWSJobParameter(parameter_id="name", value="Ahmed")]
    )
    results = [UWSJobResult(result_id="greeting", url="https://example.com")]
    await job_service.start("user", job.job_id, "some-token")
    job = await job_service.get("user", job.job_id)
    assert job.start_time is None
    assert job.phase == ExecutionPhase.QUEUED

    # Construct the arq configuration and check it.
    settings = uws.build_worker(logger)
    assert len(settings.functions) == 2
    job_started = settings.functions[0]
    assert callable(job_started)
    job_completed = settings.functions[1]
    assert callable(job_completed)
    assert settings.redis_settings == uws_config.arq_redis_settings
    assert settings.queue_name == UWS_QUEUE_NAME
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
    assert mock_slack.messages == []

    def nonnegative(value: int) -> None:
        if value < 0:
            raise ValueError("Value not nonnegative")

    def make_exception() -> None:
        try:
            nonnegative(-1)
        except Exception as e:
            raise WorkerFatalError(
                "Something", "went wrong", add_traceback=True
            ) from e

    # Test starting and erroring a job with a TaskError.
    job = await job_service.create(
        "user", params=[UWSJobParameter(parameter_id="name", value="Ahmed")]
    )
    await job_service.start("user", job.job_id, "some-token")
    job = await job_service.get("user", job.job_id)
    assert job.message_id
    await arq_queue.set_in_progress(job.message_id)
    await job_started(ctx, job.job_id, now)
    try:
        make_exception()
    except WorkerFatalError as e:
        error = e
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
    assert job.error.detail
    assert "went wrong" in job.error.detail
    assert error.traceback
    assert error.traceback in job.error.detail
    assert mock_slack.messages == [
        {
            "blocks": [
                {
                    "text": {
                        "text": "Error in vo-cutouts-db-worker: Something",
                        "type": "mrkdwn",
                        "verbatim": True,
                    },
                    "type": "section",
                },
                {
                    "fields": [
                        {
                            "text": "*Exception type*\nTaskError",
                            "type": "mrkdwn",
                            "verbatim": True,
                        },
                        {
                            "text": "*UWS job ID*\n2",
                            "type": "mrkdwn",
                            "verbatim": True,
                        },
                        {"text": ANY, "type": "mrkdwn", "verbatim": True},
                        {"text": ANY, "type": "mrkdwn", "verbatim": True},
                        {
                            "text": "*User*\nuser",
                            "type": "mrkdwn",
                            "verbatim": True,
                        },
                    ],
                    "type": "section",
                },
                {
                    "text": {
                        "text": "*Original exception*\nValueError",
                        "type": "mrkdwn",
                        "verbatim": True,
                    },
                    "type": "section",
                },
                {
                    "text": {
                        "text": "*Detail*\nwent wrong",
                        "type": "mrkdwn",
                        "verbatim": True,
                    },
                    "type": "section",
                },
            ],
            "attachments": [
                {
                    "blocks": [
                        {
                            "text": {
                                "text": (
                                    f"*Traceback*\n```\n{error.traceback}```"
                                ),
                                "type": "mrkdwn",
                                "verbatim": True,
                            },
                            "type": "section",
                        }
                    ]
                }
            ],
        },
    ]

    # Test starting and erroring a job with an unknown exception.
    mock_slack.messages = []
    job = await job_service.create(
        "user", params=[UWSJobParameter(parameter_id="name", value="Ahmed")]
    )
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
    assert job.error.error_type == ErrorType.FATAL
    assert job.error.error_code == ErrorCode.ERROR
    assert job.error.message == "Unknown error executing task"
    assert job.error.detail == "ValueError: some error"
    assert mock_slack.messages == [
        {
            "blocks": [
                {
                    "text": {
                        "text": "Uncaught exception in vo-cutouts-db-worker",
                        "type": "mrkdwn",
                        "verbatim": True,
                    },
                    "type": "section",
                },
                {
                    "fields": [
                        {
                            "text": "*Exception type*\nValueError",
                            "type": "mrkdwn",
                            "verbatim": True,
                        },
                        {"text": ANY, "type": "mrkdwn", "verbatim": True},
                    ],
                    "type": "section",
                },
                {
                    "text": {
                        "text": (
                            "*Exception*\n```\nValueError: some error\n```"
                        ),
                        "type": "mrkdwn",
                        "verbatim": True,
                    },
                    "type": "section",
                },
                {"type": "divider"},
            ],
        },
    ]
