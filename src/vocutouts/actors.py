"""Actor definitions for the image cutout service.

There are two types of actors defined here: actors that perform cutouts, and
three actors (``job_started``, ``job_completed``, and ``job_failed``) that are
simple wrappers around UWS defined here so that they can be aware of the
broker used for the image cutout service and share a session pool.

Notes
-----
There are three processes in play once the service is running: the frontend,
which will only dispatch messages and never process them; the UWS worker
process, which handles messages for the ``job_*`` actors defined here; and
the image cutout process, which handles all messages to the cutout actors.

The cutout actors here are only stubs that will never be run.  They have to
exist to be registered with the broker so that the frontend can send messages
to them.  The actual implementation is in :py:mod:`vocutouts.workers`, which
is never loaded into the frontend and only run in a stack container.

Similarly, :py:mod:`vocutouts.workers` contains stub definitions of the
``job_*`` actors so that it can dispatch messages to them, but they're only
started in the UWS worker process.

The UWS worker process and the frontend can share code.  The cutout worker
process does not; it uses only the :py:mod:`vocutouts.workers` file.

If, like me, you constantly forget Python's bizarre handling of global
variables imported from another module, remember that
``broker.worker_session`` has to be referred to that way, via the imported
module namespace.  If ``worker_session`` were imported directly, it would
always appear to be `None` because the import won't pick up changes from the
`vocutouts.broker.WorkerSession` middleware.
"""

from __future__ import annotations

from typing import Any

import dramatiq
import structlog

from . import broker
from .config import config
from .uws.jobs import uws_job_completed, uws_job_failed, uws_job_started
from .uws.utils import parse_isodatetime

__all__ = [
    "cutout",
    "job_completed",
    "job_failed",
    "job_started",
]


@dramatiq.actor(queue_name="cutout", max_retries=1, store_results=True)
def cutout(
    job_id: str,
    dataset_ids: list[str],
    stencils: list[dict[str, Any]],
) -> list[dict[str, str]]:
    """Stub for a circle cutout.

    This is only a stub, existing to define the actor in the Dramatiq broker
    used by the API frontend.  The actual implementation is in
    :py:mod:`vocutouts.workers` and must have the same signature.

    Parameters
    ----------
    job_id : `str`
        The UWS job ID, used as the key for storing results.
    dataset_ids : list[`str`]
        The data objects on which to perform cutouts.  These are opaque
        identifiers passed as-is to the backend.  The user will normally
        discover them via some service such as ObsTAP.
    stencils : list[dict[`str`, Any]]
        Serialized stencils for the cutouts to perform.  These are
        JSON-serializable (a requirement for Dramatiq) representations of the
        `~vocutouts.models.stencils.Stencil` objects corresponding to the
        user's request.

    Returns
    -------
    result : list[dict[`str`, `str`]]
        The results of the job.  This must be a list of dict representations
        of `~vocutouts.uws.models.JobResult` objects.

    Notes
    -----
    For the time being, retrying cutouts is disabled by setting
    ``max_retries`` to 1 in the actor definition.  This may not be the
    behavior we want for the production cutout service, but if we change this,
    we need to think about the case where the cutout fails, the error is
    logged in the job, and then the retry succeeds and we end up with a valid
    result.  The UWS database layer currently will not handle this properly.
    """
    return []


@dramatiq.actor(queue_name="uws", priority=0)
def job_started(job_id: str, message_id: str, start_time: str) -> None:
    """Wrapper around the UWS function to mark a job as started.

    Notes
    -----
    We would prefer to record the job started before we record when it
    finished, so this actor is set to a higher priority than completion or
    failure.  This also fixes some otherwise-annoying timing issues in the
    test suite, where a job will be marked as completed but won't have a start
    time (and hopefully will fix the same issue in a production deployment).
    """
    logger = structlog.get_logger(config.logger_name)
    start = parse_isodatetime(start_time)
    assert broker.worker_session, "Worker database connection not initalized"
    assert start, f"Invalid start timestamp {start_time}"
    uws_job_started(job_id, message_id, start, broker.worker_session, logger)


@dramatiq.actor(queue_name="uws", priority=10)
def job_completed(
    message: dict[str, Any], result: list[dict[str, str]]
) -> None:
    """Wrapper around the UWS function to mark a job as completed."""
    logger = structlog.get_logger(config.logger_name)
    job_id = message["args"][0]
    assert broker.worker_session, "Worker database connection not initalized"
    uws_job_completed(job_id, result, broker.worker_session, logger)


@dramatiq.actor(queue_name="uws", priority=20)
def job_failed(message: dict[str, Any], exception: dict[str, str]) -> None:
    """Wrapper around the UWS function to mark a job as errored."""
    logger = structlog.get_logger(config.logger_name)
    job_id = message["args"][0]
    assert broker.worker_session, "Worker database connection not initalized"
    uws_job_failed(job_id, exception, broker.worker_session, logger)
