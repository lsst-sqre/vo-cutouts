"""Task execution API.

A UWS job will have four associated classes of actors:

#. The set of actors that do the work of the job, which take a
   (JSON-serializable) list of arguments converted from the job arguments.

#. An actor that marks the UWS job as executing, invoked by the first type of
   actor as soon as they start working on a job.

#. An actor that marks the UWS job as complete and saves the metadata about
   the result to the UWS database.

#. An actor that marks the UWS job as failed and saves the error message in
   the UWS database.

The UWS library cannot define any workers directly because it doesn't know
what broker on which to register them.  However, it can provide all of the
code required for the last three types of workers, allowing the
implementations in the application to be short stubs that set up the required
configuration, database session, and logger, and then call into the functions
provided in this module.

These functions will run in an entirely separate process from the rest of the
service, so will not have the FastAPI application available.  They must create
or receive their own infrastructure objects.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy.orm import scoped_session
from structlog.stdlib import BoundLogger

from .exceptions import TaskError, UnknownJobError
from .models import JobResult
from .storage import WorkerJobStore

__all__ = [
    "uws_job_started",
    "uws_job_completed",
    "uws_job_failed",
]


def uws_job_started(
    job_id: str,
    message_id: str,
    start_time: datetime,
    session: scoped_session,
    logger: BoundLogger,
) -> None:
    """Mark a UWS job as executing.

    Parameters
    ----------
    job_id
        The identifier of the job that was started.
    message_id
        The message ID of the Dramatiq message that started it.
    start_time
        When the job was started.
    session
        A synchronous session to the UWS database.
    logger
        Logger for any messages.
    """
    storage = WorkerJobStore(session)
    try:
        storage.start_executing(job_id, message_id, start_time)
        logger.info("Marked job as started", job_id=job_id)
    except UnknownJobError:
        pass


def uws_job_completed(
    job_id: str,
    result: list[dict[str, Any]],
    session: scoped_session,
    logger: BoundLogger,
) -> None:
    """Mark a UWS job as successfully complete.

    Parameters
    ----------
    job_id
        The identifier of the job that was started.
    result
        The results of the job.  This must be a list of dict representations
        of `~vocutouts.uws.models.JobResult` objects.
    session
        A synchronous session to the UWS database.
    logger
        Logger for any messages.
    """
    storage = WorkerJobStore(session)
    job_results = [JobResult(**r) for r in result]
    try:
        storage.mark_completed(job_id, job_results)
        logger.info("Marked job as completed", job_id=job_id)
    except UnknownJobError:
        pass


def uws_job_failed(
    job_id: str,
    exception: dict[str, str],
    session: scoped_session,
    logger: BoundLogger,
) -> None:
    """Mark a UWS job as failed.

    Parameters
    ----------
    job_id
        The identifier of the job that was started.
    exception
        Exception information as passed to a Dramatiq ``on_failure`` callback.
    session
        A synchronous session to the UWS database.
    logger
        Logger for any messages.
    """
    storage = WorkerJobStore(session)
    error = TaskError.from_callback(exception).to_job_error()
    try:
        storage.mark_errored(job_id, error)
        logger.info(
            "Marked job as failed",
            job_id=job_id,
            error_type=error.error_type.value,
            error_code=error.error_code.value,
            message=error.message,
            detail=error.detail,
        )
    except UnknownJobError:
        pass
