"""The worker that performs the actual cutout."""

from __future__ import annotations

from typing import TYPE_CHECKING

import dramatiq
import structlog

from .config import config
from .models.parameters import CutoutParameters
from .uws.models import JobParameter, JobResult
from .uws.tasks import uws_worker

if TYPE_CHECKING:
    from typing import List

    from structlog.stdlib import BoundLogger

__all__ = ["task"]


@dramatiq.actor
def task(job_id: str) -> None:
    """Wrapper around the async worker.

    This is the `~dramatiq.Actor` function that does the actual cutout work
    (by, under the hood, dispatching it to an internal async function).  The
    results are stored in the database rather than returned.

    Parameters
    ----------
    job_id : `str`
        The identifier of the job to run.
    """
    logger = structlog.get_logger(config.logger_name)
    uws_config = config.uws_config()
    uws_worker(job_id, uws_config, logger, cutout)


def cutout(
    job_params: List[JobParameter], logger: BoundLogger
) -> List[JobResult]:
    """Do the actual cutout.

    This is a temporary stub for testing purposes.  It will be replaced by
    real backend code soon.
    """
    params = CutoutParameters.from_job_parameters(job_params)
    return [
        JobResult(
            result_id="cutout",
            url=f"https://example.com/cutout/{params.ids[0]}",
        )
    ]
