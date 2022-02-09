"""UWS policy layer for image cutouts."""

from __future__ import annotations

from datetime import datetime
from typing import List

from dramatiq import Actor, Message
from structlog.stdlib import BoundLogger

from .actors import job_completed, job_failed
from .exceptions import InvalidCutoutParameterError
from .models.parameters import CutoutParameters
from .models.stencils import RangeStencil
from .uws.exceptions import MultiValuedParameterError, ParameterError
from .uws.models import Job, JobParameter
from .uws.policy import UWSPolicy

__all__ = ["ImageCutoutPolicy"]


class ImageCutoutPolicy(UWSPolicy):
    """Policy layer for dispatching and approving changes to UWS jobs.

    For now, rejects all changes to destruction and execution duration by
    returning their current values.

    Parameters
    ----------
    actor : ``dramatiq.Actor``
         The actor to call for a job.  This simple mapping is temporary;
         eventually different types of cutouts will dispatch to different
         actors.
    logger : ``structlog.stdlib.BoundLogger``
         Logger to use to report errors when dispatching the request.
    """

    def __init__(self, actor: Actor, logger: BoundLogger) -> None:
        super().__init__()
        self._actor = actor
        self._logger = logger

    def dispatch(self, job: Job) -> Message:
        """Dispatch a cutout request to the backend.

        Parameters
        ----------
        job : `vocutouts.uws.models.Job`
            The submitted job description.

        Returns
        -------
        message : `dramatiq.Message`
            The dispatched message to the backend.

        Notes
        -----
        Currently, only one data ID and only one stencil are supported.  This
        limitation is expected to be relaxed in a later version.
        """
        cutout_params = CutoutParameters.from_job_parameters(job.parameters)
        return self._actor.send_with_options(
            args=(
                job.job_id,
                cutout_params.ids,
                [s.to_dict() for s in cutout_params.stencils],
            ),
            time_limit=job.execution_duration * 1000,
            on_success=job_completed,
            on_failure=job_failed,
        )

    def validate_destruction(
        self, destruction: datetime, job: Job
    ) -> datetime:
        return job.destruction_time

    def validate_execution_duration(
        self, execution_duration: int, job: Job
    ) -> int:
        return job.execution_duration

    def validate_params(self, params: List[JobParameter]) -> None:
        try:
            cutout_params = CutoutParameters.from_job_parameters(params)
        except InvalidCutoutParameterError as e:
            raise ParameterError(str(e)) from e

        # For now, only support a single ID and stencil.
        if len(cutout_params.ids) != 1:
            raise MultiValuedParameterError("Only one ID supported")
        if len(cutout_params.stencils) != 1:
            raise MultiValuedParameterError("Only one stencil is supported")

        # For now, range stencils are not supported.
        stencil = cutout_params.stencils[0]
        if isinstance(stencil, RangeStencil):
            raise ParameterError("RANGE stencils are not supported")
