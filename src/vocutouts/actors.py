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

from typing import TYPE_CHECKING

import dramatiq
import structlog

from . import broker
from .config import config
from .uws.jobs import uws_job_completed, uws_job_failed, uws_job_started
from .uws.utils import parse_isodatetime

if TYPE_CHECKING:
    from typing import Any, Dict, List


@dramatiq.actor(queue_name="cutout")
def cutout_range(
    job_id: str,
    data_id: Dict[str, str],
    ra_min: float,
    ra_max: float,
    dec_min: float,
    dec_max: float,
) -> List[Dict[str, Any]]:
    """Stub for a range cutout.

    This is only a stub, existing to define the actor in the Dramatiq broker
    used by the API frontend.  The actual implementation is in
    :py:mod:`vocutouts.workers` and must have the same signature.
    """
    pass


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
    message: Dict[str, Any], result: List[Dict[str, str]]
) -> None:
    """Wrapper around the UWS function to mark a job as completed."""
    logger = structlog.get_logger(config.logger_name)
    job_id = message["args"][0]
    assert broker.worker_session, "Worker database connection not initalized"
    uws_job_completed(job_id, result, broker.worker_session, logger)


@dramatiq.actor(queue_name="uws", priority=20)
def job_failed(message: Dict[str, Any], exception: Dict[str, str]) -> None:
    """Wrapper around the UWS function to mark a job as errored."""
    logger = structlog.get_logger(config.logger_name)
    job_id = message["args"][0]
    assert broker.worker_session, "Worker database connection not initalized"
    uws_job_failed(job_id, exception, broker.worker_session, logger)
