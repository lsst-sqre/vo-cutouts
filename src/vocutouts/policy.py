"""UWS policy layer for image cutouts."""

from __future__ import annotations

from typing import TYPE_CHECKING

from .exceptions import InvalidCutoutParameterError
from .models.parameters import CutoutParameters
from .uws.exceptions import ParameterError
from .uws.policy import UWSPolicy

if TYPE_CHECKING:
    from datetime import datetime
    from typing import List

    from .uws.models import Job, JobParameter

__all__ = ["ImageCutoutPolicy"]


class ImageCutoutPolicy(UWSPolicy):
    """Policy layer for approving changes to UWS jobs.

    For now, this does a test parse of new parameters and otherwise rejects
    all changes by returning their current values.
    """

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
            CutoutParameters.from_job_parameters(params)
        except InvalidCutoutParameterError as e:
            raise ParameterError(str(e)) from e
