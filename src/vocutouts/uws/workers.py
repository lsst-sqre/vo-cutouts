"""Define arq workers for an application using UWS."""

from __future__ import annotations

import asyncio
import contextlib
import functools
import uuid
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Concatenate, ParamSpec
from urllib.parse import urlsplit

from arq import func
from arq.connections import RedisSettings
from arq.constants import default_queue_name
from arq.typing import SecondsTimedelta, StartupShutdown, WorkerCoroutine
from arq.worker import Function
from safir.arq import (
    ArqMode,
    ArqQueue,
    JobNotFound,
    JobResultUnavailable,
    MockArqQueue,
    RedisArqQueue,
)
from safir.database import create_async_session, create_database_engine
from safir.datetime import format_datetime_for_logging
from sqlalchemy.ext.asyncio import async_scoped_session
from structlog.stdlib import BoundLogger

from .config import UWSConfig
from .constants import JOB_RESULT_TIMEOUT, UWS_QUEUE_NAME
from .exceptions import UnknownJobError
from .models import ErrorCode, ErrorType, UWSJobError, UWSJobResult
from .storage import JobStore

P = ParamSpec("P")

__all__ = [
    "WorkerSettings",
    "UWSWorkerConfig",
    "build_worker",
    "build_uws_worker",
]


@dataclass
class UWSWorkerConfig:
    """Minimal configuration needed for building a UWS backend worker."""

    arq_mode: ArqMode
    """What mode to use for the arq queue."""

    arq_queue_url: str
    """URL of the Redis arq queue."""

    arq_queue_password: str | None
    """Password of the Redis arq queue."""

    timeout: timedelta
    """Maximum execution time.

    Jobs that run longer than this length of time will be automatically
    aborted.
    """

    @property
    def arq_redis_settings(self) -> RedisSettings:
        """Redis settings for arq."""
        database = 0
        url = urlsplit(self.arq_queue_url)
        if url.path:
            database = int(url.path.lstrip("/"))
        return RedisSettings(
            host=url.hostname or "localhost",
            port=url.port or 6379,
            database=database,
            password=self.arq_queue_password,
        )


@dataclass
class WorkerSettings:
    """Configuration class for an arq worker.

    The arq command-line tool reads a class of the name ``WorkerSettings`` in
    the module it was given on the command line and turns its attributes into
    parameters to `arq.worker.Worker`. This dataclass is not a valid
    configuration class for arq; it exists only to define the contents of the
    class returned by other functions.
    """

    functions: Sequence[Function | WorkerCoroutine]
    """Coroutines to register as arq worker entry points."""

    redis_settings: RedisSettings
    """Redis configuration for arq."""

    job_timeout: SecondsTimedelta
    """Timeout for all jobs."""

    queue_name: str = default_queue_name
    """Name of arq queue to listen to for jobs."""

    on_startup: StartupShutdown | None = None
    """Coroutine to run on startup."""

    on_shutdown: StartupShutdown | None = None
    """Coroutine to run on shutdown."""

    allow_abort_jobs: bool = False
    """Whether to allow jobs to be aborted."""


def build_worker(
    worker: Callable[Concatenate[str, P], list[UWSJobResult]],
    config: UWSWorkerConfig,
    logger: BoundLogger,
) -> WorkerSettings:
    """Construct an arq worker for the provided backend function.

    Builds an arq worker configuration that wraps the provided sync function
    and executes it on messages to the default arq queue. Messages to the UWS
    queue will be sent on job start and after job completion so that the UWS
    database can be updated.

    Unfortunately, the built-in arq ``on_job_start`` and ``after_job_end``
    hooks can't be used because they don't receive any arguments to the job
    and we need to tell the UWS handlers the job ID to act on. This means that
    we'll send the UWS queue message before the results are recorded in Redis,
    so the UWS handler has to deal with that.

    Parameters
    ----------
    worker
        Synchronous function that does the actual work. This function will be
        run in a thread pool of size one.
    config
        UWS worker configuration.
    logger
        Logger to use for messages.
    """

    async def startup(ctx: dict[Any, Any]) -> None:
        nonlocal logger
        logger = logger.bind(worker_instance=uuid.uuid4().hex)

        # The queue to which to send UWS notification messages.
        if config.arq_mode == ArqMode.production:
            settings = config.arq_redis_settings
            arq: ArqQueue = await RedisArqQueue.initialize(
                settings, default_queue_name=UWS_QUEUE_NAME
            )
        else:
            arq = MockArqQueue(default_queue_name=UWS_QUEUE_NAME)

        ctx["arq"] = arq
        ctx["logger"] = logger
        ctx["pool"] = ThreadPoolExecutor(1)

        logger.info("Worker startup complete")

    async def shutdown(ctx: dict[Any, Any]) -> None:
        logger: BoundLogger = ctx["logger"]
        pool: ThreadPoolExecutor = ctx["pool"]

        pool.shutdown(wait=True, cancel_futures=True)

        logger.info("Worker shutdown complete")

    async def run(
        ctx: dict[Any, Any], job_id: str, *args: P.args, **kwargs: P.kwargs
    ) -> list[UWSJobResult]:
        arq: ArqQueue = ctx["arq"]
        logger: BoundLogger = ctx["logger"].bind(
            task=worker.__qualname__, job_id=job_id
        )
        pool: ThreadPoolExecutor = ctx["pool"]

        await arq.enqueue("job_started", job_id, datetime.now(tz=UTC))
        loop = asyncio.get_running_loop()
        worker_call = functools.partial(
            worker, job_id, *args, **kwargs, logger=logger
        )
        try:
            return await loop.run_in_executor(pool, worker_call)
        finally:
            await arq.enqueue("job_completed", job_id)

    return WorkerSettings(
        functions=[func(run, name=worker.__qualname__)],
        redis_settings=config.arq_redis_settings,
        job_timeout=config.timeout,
        on_startup=startup,
        on_shutdown=shutdown,
        allow_abort_jobs=True,
    )


async def job_started(
    ctx: dict[Any, Any], job_id: str, start_time: datetime
) -> None:
    """Mark a UWS job as executing.

    Parameters
    ----------
    job_id
        UWS job identifier.
    start_time
        When the job was started.
    """
    logger: BoundLogger = ctx["logger"].bind(task="job_started", job_id=job_id)
    storage: JobStore = ctx["storage"]

    try:
        await storage.mark_executing(job_id, start_time)
        logger.info(
            "Marked job as started",
            start_time=format_datetime_for_logging(start_time),
        )
    except UnknownJobError:
        logger.warning("Job not found to mark as started", job_id=job_id)


async def job_completed(ctx: dict[Any, Any], job_id: str) -> None:
    """Mark a UWS job as completed.

    Recover the exception if the job failed and record that as the job error.
    Because we can't use the arq ``after_job_end`` callback, the job results
    may not be available yet when we're called, which requires polling.

    Parameters
    ----------
    job_id
        UWS job identifier.
    """
    arq: ArqQueue = ctx["arq"]
    logger: BoundLogger = ctx["logger"].bind(
        task="job_completed", job_id=job_id
    )
    storage: JobStore = ctx["storage"]

    try:
        job = await storage.get(job_id)
    except UnknownJobError:
        logger.warning("Job not found to mark as completed")
        return
    arq_job_id = job.message_id
    if not arq_job_id:
        logger.error("Job has no associated arq job ID, cannot mark completed")
        return
    logger = logger.bind(arq_job_id=arq_job_id)

    try:
        now = datetime.now(tz=UTC)
        end = now + JOB_RESULT_TIMEOUT
        result = None
        while now < end:
            await asyncio.sleep(0.5)
            with contextlib.suppress(JobResultUnavailable):
                result = await arq.get_job_result(arq_job_id)
                break
        if not result:
            result = await arq.get_job_result(arq_job_id)
    except (JobNotFound, JobResultUnavailable) as e:
        logger.exception("Cannot retrieve job result")
        error = UWSJobError(
            error_type=ErrorType.TRANSIENT,
            error_code=ErrorCode.SERVICE_UNAVAILABLE,
            message="Cannot retrieve job result from job queue",
            detail=f"{type(e).__name__}: {e!s}",
        )
        try:
            await storage.mark_failed(job_id, error)
            logger.info("Marked job as failed")
        except UnknownJobError:
            logger.warning("Job not found to mark as failed")
        return

    try:
        await storage.mark_completed(job_id, result)
        logger.info("Marked job as completed")
    except UnknownJobError:
        logger.warning("Job not found to mark as completed")


def build_uws_worker(config: UWSConfig, logger: BoundLogger) -> WorkerSettings:
    """Construct an arq worker configuration for the UWS tracking worker.

    All UWS job status and results must be stored in the underlying database,
    since the API serves job information from there. To minimize dependencies
    for the worker, which may (for example) pin its own version of SQLAlchemy
    that may not be compatible with that used by the application, the actual
    worker is not responsible for storing the results in SQL. Instead, it
    returns results via arq, which temporarily puts them in Redis then uses
    ``on_job_start`` and ``after_job_end`` to notify a different queue. Those
    results are recovered and stored in the database by separate a separate
    arq worker.

    This function returns a class suitable for being assigned to
    ``WorkerSettings``, which defines the arq worker that does this database
    state tracking.

    Parameters
    ----------
    config
        UWS configuration.
    logger
        Logger to use for messages.
    """

    async def startup(ctx: dict[Any, Any]) -> None:
        nonlocal logger
        logger = logger.bind(worker_instance=uuid.uuid4().hex)
        engine = create_database_engine(
            config.database_url,
            config.database_password,
            isolation_level="REPEATABLE READ",
        )
        session = await create_async_session(engine, logger)
        storage = JobStore(session)

        # The queue from which to retrieve results is the main work queue,
        # which uses the default arq queue name. Note that this is not the
        # separate UWS queue this worker is running against.
        if config.arq_mode == ArqMode.production:
            settings = config.arq_redis_settings
            arq: ArqQueue = await RedisArqQueue.initialize(settings)
        else:
            arq = MockArqQueue()

        ctx["arq"] = arq
        ctx["logger"] = logger
        ctx["session"] = session
        ctx["storage"] = storage

        logger.info("Worker startup complete")

    async def shutdown(ctx: dict[Any, Any]) -> None:
        logger: BoundLogger = ctx["logger"]
        session: async_scoped_session = ctx["session"]

        await session.remove()

        logger.info("Worker shutdown complete")

    return WorkerSettings(
        functions=[job_started, job_completed],
        redis_settings=config.arq_redis_settings,
        job_timeout=timedelta(seconds=30),
        queue_name=UWS_QUEUE_NAME,
        on_startup=startup,
        on_shutdown=shutdown,
    )
