"""Policy layer for changes to UWS jobs."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime

from dramatiq import Message

from .models import Job, JobParameter

__all__ = ["UWSPolicy"]


class UWSPolicy(ABC):
    """Abstract interface for the application-provided policy layer.

    This class encapsulates functions to make policy decisions about UWS
    actions specific to a particular service.  Examples include dispatching
    work to a backend worker, validating parameters when the user attempts to
    change them after job creation, or deciding whether to accept a new
    execution duration or destruction time.

    Applications that use UWS should create an implementation of this abstract
    base class and then pass it into
    `~vocutouts.uws.dependencies.UWSDepencency` ``initialize`` method.
    """

    @abstractmethod
    def dispatch(self, job: Job) -> Message:
        """Dispatch a job to a backend worker.

        This method is responsible for converting UWS job parameters to the
        appropriate arguments for a backend job and invoking it with the
        appropriate timeout.

        Parameters
        ----------
        job
            The job to start.

        Returns
        -------
        dramatiq.Message
            The message sent to the backend worker.
        """

    @abstractmethod
    def validate_destruction(
        self, destruction: datetime, job: Job
    ) -> datetime:
        """Validate a new destruction time for a job.

        Parameters
        ----------
        destruction
           New date at which the job outputs and its metadata will be
           deleted to recover resources.
        job
            The existing job.

        Returns
        -------
        datetime.datetime
            The new destruction time for the job, which should be
            ``job.destruction_time`` if the policy layer doesn't want to allow
            any change.
        """

    @abstractmethod
    def validate_execution_duration(
        self, execution_duration: int, job: Job
    ) -> int:
        """Validate a new execution duration for a job.

        Parameters
        ----------
        execution_duration
            New desired maximum execution time for the job in wall clock
            seconds.
        job
            The existing job.

        Returns
        -------
        int
            The new execution duration for the job, which should be
            ``job.execution_duration`` if the policy layer doesn't want to
            allow any change.
        """

    @abstractmethod
    def validate_params(self, params: list[JobParameter]) -> None:
        """Validate parameters for a job.

        Parameters
        ----------
        params
            The new parameters.

        Raises
        ------
        vocutouts.uws.exceptions.ParameterError
            If one of the new parameters was invalid.
        """
