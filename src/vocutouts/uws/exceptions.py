"""Exceptions for the Universal Worker Service.

The types of exceptions here control the error handling behavior configured in
:py:mod:`vocutouts.uws.errors`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .models import ErrorType, JobError

if TYPE_CHECKING:
    from typing import Optional

__all__ = [
    "DataMissingError",
    "InvalidPhaseError",
    "ParameterError",
    "PermissionDeniedError",
    "TaskError",
    "TaskFatalError",
    "TaskTransientError",
    "UnknownJobError",
    "UsageError",
]


class DataMissingError(Exception):
    """The data requested does not exist for that job."""


class UnknownJobError(DataMissingError):
    """The named job could not be found in the database."""

    def __init__(self, job_id: str) -> None:
        self.job_id = job_id
        super().__init__(f"Job {job_id} not found")


class PermissionDeniedError(Exception):
    """User does not have access to this resource."""


class TaskError(Exception):
    """An error occurred during background task processing."""

    error_type = ErrorType.FATAL

    def __init__(self, message: str, detail: Optional[str] = None) -> None:
        super().__init__(message)
        self.detail = detail

    def to_job_error(self) -> JobError:
        """Convert to a `~vocutouts.uws.models.JobError`.

        This may be overridden by child classes to provide additional
        information if desired, or `TaskFatalError` and `TaskTransientError`
        can be used as-is.
        """
        return JobError(
            message=str(self), error_type=self.error_type, detail=self.detail
        )


class TaskFatalError(TaskError):
    """Fatal error occurred during background task processing.

    The parameters or other job information was invalid and this job will
    never succeed.
    """

    error_type = ErrorType.FATAL


class TaskTransientError(TaskError):
    """Transient error occurred during background task processing.

    The job may be retried with the same parameters and may succeed.
    """

    error_type = ErrorType.TRANSIENT


class UsageError(Exception):
    """Invalid parameters were passed to a UWS API."""


class InvalidPhaseError(UsageError):
    """The job is in an invalid phase for the desired operation."""


class ParameterError(UsageError):
    """Unsupported value passed to a parameter."""
