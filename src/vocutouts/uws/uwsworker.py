"""Construction of UWS backend workers."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, TypeVar
from urllib.parse import urlsplit

from arq import func
from arq.connections import RedisSettings
from arq.constants import default_queue_name
from arq.typing import SecondsTimedelta, StartupShutdown, WorkerCoroutine
from arq.worker import Function
from pydantic import BaseModel
from safir.arq import ArqMode, ArqQueue, MockArqQueue, RedisArqQueue
from structlog.stdlib import BoundLogger

from .constants import UWS_QUEUE_NAME

T = TypeVar("T", bound="BaseModel")

__all__ = [
    "WorkerConfig",
    "WorkerJobInfo",
    "WorkerResult",
    "WorkerSettings",
    "build_worker",
]


@dataclass
class WorkerConfig:
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
    """Maximum timeout for all jobs."""

    queue_name: str = default_queue_name
    """Name of arq queue to listen to for jobs."""

    on_startup: StartupShutdown | None = None
    """Coroutine to run on startup."""

    on_shutdown: StartupShutdown | None = None
    """Coroutine to run on shutdown."""

    allow_abort_jobs: bool = False
    """Whether to allow jobs to be aborted."""


@dataclass
class WorkerJobInfo:
    """Metadata about the job that may be useful to the backend."""

    job_id: str
    """UWS job identifier (not the same as the arq job ID)."""

    user: str
    """Username of the user who submitted the job."""

    token: str
    """Delegated Gafaelfawr token to act on behalf of the user."""

    timeout: timedelta
    """Maximum execution time for the job."""

    run_id: str | None = None
    """User-supplied run ID, if any."""


class WorkerResult(BaseModel):
    """A single result from the job."""

    result_id: str
    """Identifier for the result."""

    url: str
    """URL for the result, which must point to a GCS bucket."""

    size: int | None = None
    """Size of the result in bytes."""

    mime_type: str | None = None
    """MIME type of the result."""


def build_worker(
    worker: Callable[[T, WorkerJobInfo, BoundLogger], list[WorkerResult]],
    config: WorkerConfig,
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
        ctx: dict[Any, Any], params: T, info: WorkerJobInfo
    ) -> list[WorkerResult]:
        arq: ArqQueue = ctx["arq"]
        logger: BoundLogger = ctx["logger"]
        pool: ThreadPoolExecutor = ctx["pool"]

        logger = logger.bind(
            task=worker.__qualname__,
            job_id=info.job_id,
            user=info.user,
            params=params.model_dump(mode="json"),
        )
        if info.run_id:
            logger = logger.bind(run_id=info.run_id)

        await arq.enqueue("job_started", info.job_id, datetime.now(tz=UTC))
        loop = asyncio.get_running_loop()
        try:
            return await loop.run_in_executor(
                pool, worker, params, info, logger
            )
        finally:
            await arq.enqueue("job_completed", info.job_id)

    return WorkerSettings(
        functions=[func(run, name=worker.__qualname__)],
        redis_settings=config.arq_redis_settings,
        job_timeout=config.timeout,
        on_startup=startup,
        on_shutdown=shutdown,
        allow_abort_jobs=True,
    )
