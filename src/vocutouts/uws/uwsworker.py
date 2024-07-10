"""Construction of UWS backend workers."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from traceback import format_exception
from typing import Any, ClassVar, Generic, TypeVar
from urllib.parse import urlsplit

from arq import func
from arq.connections import RedisSettings
from arq.constants import default_queue_name
from arq.cron import CronJob
from arq.typing import SecondsTimedelta, StartupShutdown, WorkerCoroutine
from arq.worker import Function
from pydantic import BaseModel
from safir.arq import ArqMode, ArqQueue, MockArqQueue, RedisArqQueue
from structlog.stdlib import BoundLogger

from .constants import UWS_QUEUE_NAME

T = TypeVar("T", bound="BaseModel")

__all__ = [
    "WorkerConfig",
    "WorkerError",
    "WorkerErrorType",
    "WorkerFatalError",
    "WorkerJobInfo",
    "WorkerResult",
    "WorkerSettings",
    "WorkerTransientError",
    "WorkerUsageError",
    "build_worker",
]


@dataclass
class WorkerConfig(Generic[T]):
    """Minimal configuration needed for building a UWS backend worker."""

    arq_mode: ArqMode
    """What mode to use for the arq queue."""

    arq_queue_url: str
    """URL of the Redis arq queue."""

    arq_queue_password: str | None
    """Password of the Redis arq queue."""

    parameters_class: type[T]
    """Class of the parameters to pass to the backend worker."""

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

    max_jobs: int
    """Maximum number of jobs that can be run at one time."""

    queue_name: str = default_queue_name
    """Name of arq queue to listen to for jobs."""

    on_startup: StartupShutdown | None = None
    """Coroutine to run on startup."""

    on_shutdown: StartupShutdown | None = None
    """Coroutine to run on shutdown."""

    cron_jobs: Sequence[CronJob] | None = None
    """Cron jobs to run."""


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
    """Maximum execution time for the job.

    Currently, this is ignored, since the backend workers do not support
    cancellation.
    """

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


class WorkerErrorType(Enum):
    """Types of errors that may be reported by a worker."""

    FATAL = "fatal"
    TRANSIENT = "transient"
    USAGE = "usage"


class WorkerError(Exception):
    """An error occurred during background task processing.

    Attributes
    ----------
    cause_type
        Type of the underlying exception, if there is one.
    detail
        Additional error detail, not including the traceback if any.
    error_type
        Indicates whether this exception represents a transient error that may
        go away if the request is retried or a permanent error with the
        request.
    traceback
        Traceback of the underlying triggering exception, if tracebacks were
        requested and there is a cause set.
    user
        User whose action triggered this exception, for Slack reporting.

    Parameters
    ----------
    message
        Human-readable error message.
    detail
        Additional details about the error.
    add_traceback
        Whether to add a traceback of the underlying cause to the error
        details.
    """

    error_type: ClassVar[WorkerErrorType] = WorkerErrorType.FATAL
    """Type of error this exception represents."""

    def __init__(
        self,
        message: str,
        detail: str | None = None,
        *,
        add_traceback: bool = False,
    ) -> None:
        super().__init__(message)
        self.detail = detail
        self._cause_type: str | None = None
        self._traceback: str | None = None
        self._add_traceback = add_traceback

    def __reduce__(self) -> str | tuple:
        # Ensure the cause information is serialized before pickling.
        self._cause_type = self._serialize_cause_type()
        self._traceback = self._serialize_traceback()
        return super().__reduce__()

    @property
    def cause_type(self) -> str | None:
        """Type of the exception that triggered this error, if known."""
        if not self._cause_type:
            self._cause_type = self._serialize_cause_type()
        return self._cause_type

    @property
    def traceback(self) -> str | None:
        """Traceback of the underlying exception, if desired."""
        if not self._traceback:
            self._traceback = self._serialize_traceback()
        return self._traceback

    def _serialize_cause_type(self) -> str | None:
        """Serialize the type of exception from ``__cause__``."""
        if not self.__cause__:
            return None
        return type(self.__cause__).__qualname__

    def _serialize_traceback(self) -> str | None:
        """Serialize the traceback from ``__cause__``."""
        if not self._add_traceback or not self.__cause__:
            return None
        return "".join(format_exception(self.__cause__))


class WorkerFatalError(WorkerError):
    """Fatal error occurred during worker processing.

    The parameters or other job information was invalid and this job will
    never succeed.
    """


class WorkerTransientError(WorkerError):
    """Transient error occurred during worker processing.

    The job may be retried with the same parameters and may succeed.
    """

    error_type = WorkerErrorType.TRANSIENT


class WorkerUsageError(WorkerError):
    """Parameters sent by the user were invalid.

    The parameters or other job information was invalid and this job will
    never succeed. This is the same as `WorkerFatalError` except that it
    represents a user error and will not be reported to Slack as a service
    problem.
    """

    error_type = WorkerErrorType.USAGE


def build_worker(
    worker: Callable[[T, WorkerJobInfo, BoundLogger], list[WorkerResult]],
    config: WorkerConfig[T],
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

    Notes
    -----
    Timeouts and aborting jobs unfortunately are not supported due to
    limitations in `concurrent.futures.ThreadPoolExecutor`. Once a thread has
    been started, there is no way to stop it until it completes on its own.
    Therefore, no job timeout is set or supported, and the timeout set on the
    job (which comes from executionduration) is ignored.

    Fixing this appears to be difficult since Python's `threading.Thread`
    simply does not support cancellation. It would probably require rebuilding
    the worker model on top of processes and killing those processes on
    timeout. That would pose problems for cleanup of any temporary resources
    created by the process such as temporary files, since Python cleanup code
    would not be run.

    The best fix would be for backend code to be rewritten to be async, so
    await would become a cancellation point (although this still may not be
    enough for compute-heavy code that doesn't use await frequently). However,
    the Rubin pipelines code is all sync, so async worker support has not yet
    been added due to lack of demand.
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
        ctx: dict[Any, Any], params_raw: dict[str, Any], info: WorkerJobInfo
    ) -> list[WorkerResult]:
        arq: ArqQueue = ctx["arq"]
        logger: BoundLogger = ctx["logger"]
        pool: ThreadPoolExecutor = ctx["pool"]

        params = config.parameters_class.model_validate(params_raw)
        logger = logger.bind(
            task=worker.__qualname__,
            job_id=info.job_id,
            user=info.user,
            params=params.model_dump(mode="json"),
        )
        if info.run_id:
            logger = logger.bind(run_id=info.run_id)

        await arq.enqueue("uws_job_started", info.job_id, datetime.now(tz=UTC))
        loop = asyncio.get_running_loop()
        try:
            return await loop.run_in_executor(
                pool, worker, params, info, logger
            )
        finally:
            await arq.enqueue("uws_job_completed", info.job_id)

    # Job timeouts are not actually supported since we have no way of stopping
    # the sync worker. A timeout will just leave the previous worker running
    # and will block all future jobs. Set it to an extremely long value, since
    # it can't be disabled entirely.
    #
    # Since the worker is running sync jobs, run one job per pod since they
    # will be serialized anyway and no parallelism is possible. If async
    # worker support is added, consider making this configurable.
    return WorkerSettings(
        functions=[func(run, name=worker.__qualname__)],
        redis_settings=config.arq_redis_settings,
        job_timeout=3600,
        max_jobs=1,
        on_startup=startup,
        on_shutdown=shutdown,
    )
