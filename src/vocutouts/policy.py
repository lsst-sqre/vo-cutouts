"""UWS policy layer for image cutouts."""

from __future__ import annotations

from typing import TYPE_CHECKING

from .actors import job_completed, job_failed
from .exceptions import InvalidCutoutParameterError
from .models.parameters import CutoutParameters
from .models.stencils import RangeStencil
from .uws.exceptions import MultiValuedParameterError, ParameterError
from .uws.policy import UWSPolicy

if TYPE_CHECKING:
    from datetime import datetime
    from typing import List

    from dramatiq import Actor, Message

    from .uws.models import Job, JobParameter

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
    """

    def __init__(self, actor: Actor) -> None:
        super().__init__()
        self._actor = actor

    def dispatch(self, job: Job) -> Message:
        """Dispatch a cutout request to the backend.

        Everything about this function is preliminary, just enough to get a
        proof-of-concept working.  It will become more sophisticated once we
        agree on a data ID format and more cutout stencils are supported.
        """
        cutout_params = CutoutParameters.from_job_parameters(job.parameters)
        visit, detector, band, instrument = cutout_params.ids[0].split(":")
        data_id = {
            "visit": visit,
            "detector": detector,
            "band": band,
            "instrument": instrument,
        }
        stencil = cutout_params.stencils[0]
        assert isinstance(stencil, RangeStencil)
        ra_min, ra_max = stencil.ra
        dec_min, dec_max = stencil.dec
        return self._actor.send_with_options(
            args=(job.job_id, data_id, ra_min, ra_max, dec_min, dec_max),
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

        # For now, only range stencils are supported.
        stencil = cutout_params.stencils[0]
        if not isinstance(stencil, RangeStencil):
            raise ParameterError("Only RANGE stencils are supported")
