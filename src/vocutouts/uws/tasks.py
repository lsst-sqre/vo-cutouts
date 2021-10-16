"""Task execution API.

The application using this framework should define a Dramatiq worker that
calls `uws_worker`, passing in the function that does the actual work as
the ``worker`` argument.  `uws_worker` will then handle all of the necessary
database bookkeeping.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from dramatiq.middleware import CurrentMessage

from .database import create_sync_session
from .exceptions import TaskError, UnknownJobError
from .models import ErrorType, JobError
from .storage import WorkerJobStore

if TYPE_CHECKING:
    from typing import Callable, List, Optional

    from sqlalchemy.orm import scoped_session
    from structlog.stdlib import BoundLogger

    from .config import UWSConfig
    from .models import JobParameter, JobResult

    Worker = Callable[[List[JobParameter], BoundLogger], List[JobResult]]

__all__ = ["uws_worker"]

_SESSION: Optional[scoped_session] = None
"""The scoped Session object used by all workers."""


def uws_worker(
    job_id: str, config: UWSConfig, logger: BoundLogger, worker: Worker
) -> None:
    """Retrieve the job parameters and do the bookkeeping for the task.

    Parameters
    ----------
    job_id : `str`
        The identifier of the job to run.
    config : `vocutouts.uws.config.UWSConfig`
        Configuration for the UWS subsystem.
    logger : `structlog.stdlib.BoundLogger`
        Logger for any messages.
    worker : `typing.Callable`
        The function to call to do the actual work of the job.  It will be
        passed the list of `~vocutouts.uws.models.JobParameter` objects and
        the logger, and should return a list of
        `~vocutouts.uws.models.JobResult` objects.

    Notes
    -----
    This function will run in an entirely separate process from the rest of
    the service, so will not have the FastAPI application available.  It must
    create or receive its own infrastructure objects.
    """
    # Grab the database session or create it if we're the first worker in this
    # process to need one.  Then create the job storage layer.
    global _SESSION
    if not _SESSION:
        _SESSION = create_sync_session(config, logger)
    storage = WorkerJobStore(_SESSION)

    # If the job no longer exists when storing results, that means it was
    # deleted in the meantime and we're happy to silently discard results, or
    # not start the job at all if we haven't yet.
    message = CurrentMessage.get_current_message()
    try:
        job = storage.start_executing(job_id, message.message_id)
        try:
            results = worker(job.parameters, logger)
        except TaskError as e:
            storage.mark_errored(job_id, e.to_job_error())
        except Exception as e:
            error = JobError(
                message="Unknown error executing task",
                error_type=ErrorType.TRANSIENT,
                detail=f"{type(e).__name__}: {str(e)}",
            )
            storage.mark_errored(job_id, error)
        else:
            storage.mark_completed(job_id, results)
    except UnknownJobError:
        pass
