"""Exceptions for the Universal Worker Service.

The types of exceptions here control the error handling behavior configured in
:py:mod:`vocutouts.uws.errors`.
"""

from __future__ import annotations

from datetime import datetime
from traceback import format_exception
from typing import ClassVar

from safir.datetime import format_datetime_for_logging
from safir.slack.blockkit import (
    SlackCodeBlock,
    SlackException,
    SlackMessage,
    SlackTextBlock,
    SlackTextField,
)
from safir.slack.webhook import SlackIgnoredException

from .models import ErrorCode, ErrorType, UWSJobError

__all__ = [
    "DataMissingError",
    "InvalidPhaseError",
    "ParameterError",
    "PermissionDeniedError",
    "TaskError",
    "TaskFatalError",
    "TaskTransientError",
    "TaskUserError",
    "UnknownJobError",
    "UsageError",
    "UWSError",
]


class UWSError(Exception):
    """An error with an associated error code.

    SODA requires errors be in ``text/plain`` and start with an error code.
    Adopt that as a general representation of errors produced by the UWS
    layer to simplify generating error responses.

    Parameters
    ----------
    error_code
        SODA error code.
    message
        Exception message, which will be the stringification of the exception.
    detail
        Additional detail.
    """

    def __init__(
        self, error_code: ErrorCode, message: str, detail: str | None = None
    ) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.detail = detail
        self.status_code = 400


class MultiValuedParameterError(UWSError, SlackIgnoredException):
    """Multiple values not allowed for this parameter."""

    def __init__(self, message: str) -> None:
        super().__init__(ErrorCode.MULTIVALUED_PARAM_NOT_SUPPORTED, message)
        self.status_code = 422


class PermissionDeniedError(UWSError, SlackIgnoredException):
    """User does not have access to this resource."""

    def __init__(self, message: str) -> None:
        super().__init__(ErrorCode.AUTHORIZATION_ERROR, message)
        self.status_code = 403


class TaskError(SlackException):
    """An error occurred during background task processing.

    Attributes
    ----------
    error_type
        Indicates whether this exception represents a transient error that may
        go away if the request is retried or a permanent error with the
        request.
    job_id
        UWS job ID, if known.
    started_at
        When the task was started, if known.
    traceback
        Traceback of the underlying triggering exception, if tracebacks were
        requested and there is a cause set.
    user
        User whose action triggered this exception, for Slack reporting.

    Parameters
    ----------
    error_code
        DALI-compatible error code.
    message
        Human-readable error message.
    details
        Additional details about the error.
    add_traceback
        Whether to add a traceback of the underlying cause to the error
        details.
    traceback
        Expanded traceback, used to preserve the traceback across pickling.
    """

    error_type: ClassVar[ErrorType] = ErrorType.TRANSIENT
    """Type of error this exception represents."""

    def __init__(
        self,
        error_code: ErrorCode,
        message: str,
        detail: str | None = None,
        *,
        add_traceback: bool = False,
    ) -> None:
        super().__init__(message)
        self.job_id: str | None = None
        self.started_at: datetime | None = None
        self._error_code = error_code
        self._message = message
        self._detail = detail
        self._add_traceback = add_traceback
        self._cause_type: str | None = None
        self._traceback: str | None = None

    def __reduce__(self) -> str | tuple:
        # Ensure the traceback is serialized before pickling.
        self._traceback = self._serialize_traceback()
        return super().__reduce__()

    @property
    def traceback(self) -> str | None:
        """Traceback of the underlying exception, if desired."""
        if self._traceback:
            return self._traceback
        self._traceback = self._serialize_traceback()
        return self._traceback

    def to_job_error(self) -> UWSJobError:
        """Convert to a `~vocutouts.uws.models.UWSJobError`."""
        if self.traceback and self._detail:
            detail: str | None = self._detail + "\n\n" + self.traceback
        else:
            detail = self._detail or self.traceback
        return UWSJobError(
            error_code=self._error_code,
            error_type=self.error_type,
            message=str(self),
            detail=detail,
        )

    def to_slack(self) -> SlackMessage:
        message = super().to_slack()
        if self.traceback:
            block = SlackCodeBlock(heading="Traceback", code=self.traceback)
            message.attachments.append(block)
        if self.started_at:
            started_at = format_datetime_for_logging(self.started_at)
            field = SlackTextField(heading="Started at", text=started_at)
            message.fields.insert(1, field)
        if self.job_id:
            field = SlackTextField(heading="UWS job ID", text=self.job_id)
            message.fields.insert(1, field)
        if self._cause_type:
            text = SlackTextBlock(
                heading="Original exception", text=self._cause_type
            )
            message.blocks.append(text)

        if self._detail:
            text = SlackTextBlock(heading="Detail", text=self._detail)
            message.blocks.append(text)
        return message

    def _serialize_traceback(self) -> str | None:
        """Serialize the traceback from ``__cause__``."""
        if not self._add_traceback or not self.__cause__:
            return None
        self._cause_type = type(self.__cause__).__name__
        return "".join(format_exception(self.__cause__))


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


class TaskUserError(TaskFatalError, SlackIgnoredException):
    """Fatal user error occurred during background task processing.

    The parameters or other job information was invalid and this job will
    never succeed. This is the same as `TaskFatalError` except that it
    represents a user error and will not be reported to Slack as a service
    problem.
    """


class UsageError(UWSError, SlackIgnoredException):
    """Invalid parameters were passed to a UWS API."""

    def __init__(self, message: str, detail: str | None = None) -> None:
        super().__init__(ErrorCode.USAGE_ERROR, message, detail)
        self.status_code = 422


class DataMissingError(UWSError, SlackIgnoredException):
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
