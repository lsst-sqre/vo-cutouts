"""Models for UWS services.

See https://www.ivoa.net/documents/UWS/20161024/REC-UWS-1.1-20161024.html.
Descriptive language here is paraphrased from this standard.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import List, Optional


@dataclass
class Availability:
    """Availability information."""

    available: bool
    """Whether the service appears to be available."""

    note: Optional[str] = None
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


ACTIVE_PHASES = (
    ExecutionPhase.PENDING,
    ExecutionPhase.QUEUED,
    ExecutionPhase.EXECUTING,
)
"""Phases in which the job is active and can be waited on."""


class ErrorType(Enum):
    """Types of job errors."""

    TRANSIENT = "transient"
    FATAL = "fatal"


@dataclass
class JobError:
    """Failure information about a job."""

    message: str
    """Brief error message.

    Note that the UWS specification allows a sequence of messages, but we only
    use a single message and thus a sequence of length one.
    """

    error_type: ErrorType
    """Type of the error."""

    detail: Optional[str] = None
    """Extended error message with additional detail."""


@dataclass
class JobResult:
    """A single result from the job."""

    result_id: str
    """Identifier for the result."""

    url: str
    """URL to the content of the result."""

    size: Optional[int] = None
    """Size of the result in bytes."""

    mime_type: Optional[str] = None
    """MIME type of the result."""


@dataclass
class JobParameter:
    """An input parameter to the job."""

    parameter_id: str
    """Identifier of the parameter."""

    value: str
    """Value of the parameter."""

    is_post: bool = False
    """Whether the parameter was provided via POST."""


@dataclass
class JobDescription:
    """Brief job description used for the job list."""

    job_id: str
    """Unique identifier of the job."""

    owner: str
    """Identity of the owner of the job."""

    phase: ExecutionPhase
    """Execution phase of the job."""

    run_id: Optional[str]
    """Optional opaque string provided by the client.

    The RunId is intended for the client to add a unique identifier to all
    jobs that are part of a single operation from the perspective of the
    client.  This may aid in tracing issues through a complex system or
    identifying which operation a job is part of.
    """

    creation_time: datetime
    """When the job was created."""


@dataclass
class Job:
    """Represents a single UWS job."""

    job_id: str
    """Unique identifier of the job."""

    message_id: Optional[str]
    """Internal message identifier for the work queuing system."""

    owner: str
    """Identity of the owner of the job."""

    phase: ExecutionPhase
    """Execution phase of the job."""

    run_id: Optional[str]
    """Optional opaque string provided by the client.

    The RunId is intended for the client to add a unique identifier to all
    jobs that are part of a single operation from the perspective of the
    client.  This may aid in tracing issues through a complex system or
    identifying which operation a job is part of.
    """

    creation_time: datetime
    """When the job was created."""

    start_time: Optional[datetime]
    """When the job started executing (if it has started)."""

    end_time: Optional[datetime]
    """When the job stopped executing (if it has stopped)."""

    destruction_time: datetime
    """Time at which the job should be destroyed.

    At this time, the job will be aborted if it is still running, its results
    will be deleted, and all record of the job will be discarded.
    """

    execution_duration: int
    """Allowed maximum execution duration in seconds.

    This is specified in elapsed wall clock time, or 0 for unlimited execution
    time.  If the job runs for longer than this time period, it will be
    aborted.
    """

    quote: Optional[datetime]
    """Expected completion time of the job if it were started now.

    May be `None` to indicate that the expected duration of the job is not
    known.  Maybe later than the destruction time to indicate that the job is
    not possible due to resource constraints.
    """

    error: Optional[JobError]
    """Error information if the job failed."""

    parameters: List[JobParameter]
    """The parameters of the job."""

    results: List[JobResult]
    """The results of the job."""
