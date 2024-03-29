"""Exceptions for the Universal Worker Service.

The types of exceptions here control the error handling behavior configured in
:py:mod:`vocutouts.uws.errors`.
"""

from __future__ import annotations

from .models import ErrorCode, ErrorType, JobError

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
    "UWSError",
]


class UWSError(Exception):
    """An error with an associated error code.

    SODA requires errors be in ``text/plain`` and start with an error code.
    Adopt that as a general representation of errors produced by the UWS
    layer to simplify generating error responses.
    """

    def __init__(
        self, error_code: ErrorCode, message: str, detail: str | None = None
    ) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.detail = detail
        self.status_code = 400


class MultiValuedParameterError(UWSError):
    """Multiple values not allowed for this parameter."""

    def __init__(self, message: str) -> None:
        super().__init__(ErrorCode.MULTIVALUED_PARAM_NOT_SUPPORTED, message)
        self.status_code = 422


class PermissionDeniedError(UWSError):
    """User does not have access to this resource."""

    def __init__(self, message: str) -> None:
        super().__init__(ErrorCode.AUTHORIZATION_ERROR, message)
        self.status_code = 403


class TaskError(UWSError):
    """An error occurred during background task processing."""

    @classmethod
    def from_callback(cls, exception: dict[str, str]) -> TaskError:
        """Reconstitute the exception passed to an on_failure callback.

        Notes
        -----
        Unfortunately, the ``dramatiq.middleware.Callbacks`` middleware only
        provides the type of the error message and the body as strings, so we
        have to parse the body of the exception to get the structured data we
        want to store in the UWS database.
        """
        exception_type = exception["type"]
        exception_message = exception["message"]
        detail = None
        if exception_type in ("TaskFatalError", "TaskTransientError"):
            try:
                error_code_str, rest = exception_message.split(None, 1)
                error_code = ErrorCode(error_code_str)
                if "\n" in rest:
                    message, detail = rest.split("\n", 1)
                else:
                    message = rest
            except Exception:
                error_code = ErrorCode.ERROR
                message = exception_message
            if exception_type == "TaskFatalError":
                return TaskFatalError(error_code, message, detail)
            else:
                return TaskTransientError(error_code, message, detail)
        else:
            return cls(
                ErrorType.TRANSIENT,
                ErrorCode.ERROR,
                "Unknown error executing task",
                f"{exception_type}: {exception_message}",
            )

    def __init__(
        self,
        error_type: ErrorType,
        error_code: ErrorCode,
        message: str,
        detail: str | None = None,
    ) -> None:
        super().__init__(error_code, message)
        self.error_type = error_type
        self.detail = detail

    def to_job_error(self) -> JobError:
        """Convert to a `~vocutouts.uws.models.JobError`."""
        return JobError(
            error_code=self.error_code,
            error_type=self.error_type,
            message=str(self),
            detail=self.detail,
        )


class TaskFatalError(TaskError):
    """Fatal error occurred during background task processing.

    The parameters or other job information was invalid and this job will
    never succeed.
    """

    def __init__(
        self, error_code: ErrorCode, message: str, detail: str | None = None
    ) -> None:
        super().__init__(ErrorType.FATAL, error_code, message, detail)


class TaskTransientError(TaskError):
    """Transient error occurred during background task processing.

    The job may be retried with the same parameters and may succeed.
    """

    def __init__(
        self, error_code: ErrorCode, message: str, detail: str | None = None
    ) -> None:
        super().__init__(ErrorType.TRANSIENT, error_code, message, detail)


class UsageError(UWSError):
    """Invalid parameters were passed to a UWS API."""

    def __init__(self, message: str, detail: str | None = None) -> None:
        super().__init__(ErrorCode.USAGE_ERROR, message, detail)
        self.status_code = 422


class DataMissingError(UWSError):
    """The data requested does not exist for that job."""

    def __init__(self, message: str) -> None:
        super().__init__(ErrorCode.USAGE_ERROR, message)
        self.status_code = 404


class InvalidPhaseError(UsageError):
    """The job is in an invalid phase for the desired operation."""


class ParameterError(UsageError):
    """Unsupported value passed to a parameter."""


class UnknownJobError(DataMissingError):
    """The named job could not be found in the database."""

    def __init__(self, job_id: str) -> None:
        super().__init__(f"Job {job_id} not found")
        self.job_id = job_id
