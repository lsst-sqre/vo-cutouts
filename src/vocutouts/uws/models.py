"""Models for UWS services.

See https://www.ivoa.net/documents/UWS/20161024/REC-UWS-1.1-20161024.html.
Descriptive language here is paraphrased from this standard.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from enum import Enum

__all__ = [
    "Availability",
    "ErrorCode",
    "ExecutionPhase",
    "ErrorType",
    "UWSJob",
    "UWSJobDescription",
    "UWSJobError",
    "UWSJobParameter",
    "UWSJobResult",
    "UWSJobResultURL",
]


@dataclass
class Availability:
    """Availability information (from VOSI)."""

    available: bool
    """Whether the service appears to be available."""

    note: str | None = None
    """Supplemental information, usually when the service is not available."""


class ExecutionPhase(Enum):
    """Possible execution phases for a UWS job."""

    PENDING = "PENDING"
    """Accepted by the service but not yet sent for execution."""

    QUEUED = "QUEUED"
    """Sent for execution but not yet started."""

    EXECUTING = "EXECUTING"
    """Currently in progress."""

    COMPLETED = "COMPLETED"
    """Completed and the results are available for retrieval."""

    ERROR = "ERROR"
    """Failed and reported an error."""

    ABORTED = "ABORTED"
    """Aborted before it completed."""

    UNKNOWN = "UNKNOWN"
    """In an unknown state."""

    HELD = "HELD"
    """Similar to PENDING, held and not sent for execution."""

    SUSPENDED = "SUSPENDED"
    """Execution has started, is currently suspended, and will be resumed."""

    ARCHIVED = "ARCHIVED"
    """Execution completed some time ago and the results have been deleted."""


ACTIVE_PHASES = {
    ExecutionPhase.PENDING,
    ExecutionPhase.QUEUED,
    ExecutionPhase.EXECUTING,
}
"""Phases in which the job is active and can be waited on."""


class ErrorCode(Enum):
    """Possible error codes in ``text/plain`` SODA errors."""

    AUTHENTICATION_ERROR = "AuthenticationError"
    AUTHORIZATION_ERROR = "AuthorizationError"
    MULTIVALUED_PARAM_NOT_SUPPORTED = "MultiValuedParamNotSupported"
    ERROR = "Error"
    SERVICE_UNAVAILABLE = "ServiceUnavailable"
    USAGE_ERROR = "UsageError"


class ErrorType(Enum):
    """Types of job errors."""

    TRANSIENT = "transient"
    FATAL = "fatal"


@dataclass
class UWSJobError:
    """Failure information about a job."""

    error_type: ErrorType
    """Type of the error."""

    error_code: ErrorCode
    """The SODA error code of this error."""

    message: str
    """Brief error message.

    Note that the UWS specification allows a sequence of messages, but we only
    use a single message and thus a sequence of length one.
    """

    detail: str | None = None
    """Extended error message with additional detail."""


@dataclass
class UWSJobResult:
    """A single result from the job."""

    result_id: str
    """Identifier for the result."""

    url: str
    """The URL for the result, which must point into a GCS bucket."""

    size: int | None = None
    """Size of the result in bytes."""

    mime_type: str | None = None
    """MIME type of the result."""


@dataclass
class UWSJobResultURL:
    """A single result from the job with a signed URL.

    A `UWSJobResult` is converted to a `UWSJobResultURL` before generating the
    response via templating.
    """

    result_id: str
    """Identifier for the result."""

    url: str
    """Signed URL to retrieve the result."""

    size: int | None = None
    """Size of the result in bytes."""

    mime_type: str | None = None
    """MIME type of the result."""


@dataclass
class UWSJobParameter:
    """An input parameter to the job."""

    parameter_id: str
    """Identifier of the parameter."""

    value: str
    """Value of the parameter."""

    is_post: bool = False
    """Whether the parameter was provided via POST."""

    def to_dict(self) -> dict[str, str | bool]:
        """Convert to a dictionary, primarily for logging."""
        return asdict(self)


@dataclass
class UWSJobDescription:
    """Brief job description used for the job list.

    This is a strict subset of the fields of `UWSJob`, but is kept separate
    without an inheritance relationship to reflect how it's used in code.
    """

    job_id: str
    """Unique identifier of the job."""

    owner: str
    """Identity of the owner of the job."""

    phase: ExecutionPhase
    """Execution phase of the job."""

    run_id: str | None
    """Optional opaque string provided by the client.

    The RunId is intended for the client to add a unique identifier to all
    jobs that are part of a single operation from the perspective of the
    client.  This may aid in tracing issues through a complex system or
    identifying which operation a job is part of.
    """

    creation_time: datetime
    """When the job was created."""


@dataclass
class UWSJob:
    """Represents a single UWS job."""

    job_id: str
    """Unique identifier of the job."""

    message_id: str | None
    """Internal message identifier for the work queuing system."""

    owner: str
    """Identity of the owner of the job."""

    phase: ExecutionPhase
    """Execution phase of the job."""

    run_id: str | None
    """Optional opaque string provided by the client.

    The RunId is intended for the client to add a unique identifier to all
    jobs that are part of a single operation from the perspective of the
    client. This may aid in tracing issues through a complex system or
    identifying which operation a job is part of.
    """

    creation_time: datetime
    """When the job was created."""

    start_time: datetime | None
    """When the job started executing (if it has started)."""

    end_time: datetime | None
    """When the job stopped executing (if it has stopped)."""

    destruction_time: datetime
    """Time at which the job should be destroyed.

    At this time, the job will be aborted if it is still running, its results
    will be deleted, and all record of the job will be discarded.

    This field is optional in the UWS standard, but in this UWS implementation
    all jobs will have a destruction time, so it is not marked as optional.
    """

    execution_duration: int
    """Allowed maximum execution duration in seconds.

    This is specified in elapsed wall clock time, or 0 for unlimited execution
    time. If the job runs for longer than this time period, it will be
    aborted.
    """

    quote: datetime | None
    """Expected completion time of the job if it were started now.

    May be `None` to indicate that the expected duration of the job is not
    known. Maybe later than the destruction time to indicate that the job is
    not possible due to resource constraints.
    """

    error: UWSJobError | None
    """Error information if the job failed."""

    parameters: list[UWSJobParameter]
    """The parameters of the job."""

    results: list[UWSJobResult]
    """The results of the job."""
