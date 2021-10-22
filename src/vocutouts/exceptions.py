"""Exceptions for the image cutout service."""

from __future__ import annotations

from typing import TYPE_CHECKING

from .uws.exceptions import ParameterError, TaskFatalError
from .uws.models import JobError

if TYPE_CHECKING:
    from typing import List

    from .uws.models import JobParameter

__all__ = ["InvalidCutoutParameterError"]


class InvalidCutoutParameterError(ParameterError, TaskFatalError):
    """The parameters for the cutout were invalid."""

    def __init__(self, msg: str, params: List[JobParameter]) -> None:
        super().__init__(msg)
        self.params = params

    def to_job_error(self) -> JobError:
        """Convert the exception to a `~vocutouts.uws.models.JobError`."""
        params = "\n".join(f"{p.parameter_id}={p.value}" for p in self.params)
        return JobError(
            message=str(self), error_type=self.error_type, detail=params
        )
