"""Policy layer for changes to UWS jobs."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime

from safir.arq import ArqQueue, JobMetadata

from .models import UWSJob, UWSJobParameter

__all__ = ["UWSPolicy"]


class UWSPolicy(ABC):
    """Abstract interface for the application-provided policy layer.

    This class encapsulates functions to make policy decisions about UWS
    actions specific to a particular service. Examples include dispatching
    work to a backend worker, validating parameters when the user attempts to
    change them after job creation, or deciding whether to accept a new
    execution duration or destruction time.

    Applications that use UWS should create an implementation of this abstract
    base class and then pass it into
    `~vocutouts.uws.dependencies.UWSDepencency` ``initialize`` method.

    Parameters
    ----------
    config
        UWS configuration.

    Attributes
    ----------
    arq
        arq queue client to use for dispatching jobs.
    """

    def __init__(self, arq: ArqQueue) -> None:
        self.arq = arq

    @abstractmethod
    async def dispatch(self, job: UWSJob, access_token: str) -> JobMetadata:
        """Dispatch a job to a backend worker.

        This method is responsible for converting UWS job parameters to the
        appropriate arguments for a backend job and invoking it with the
        appropriate timeout.

        Parameters
        ----------
        job
            Job to start.
        access_token
            Gafaelfawr access token used to authenticate to services used
            by the backend on the user's behalf.

        Returns
        -------
        JobMetadata
            Message sent to the backend worker.
        """

    def validate_destruction(
        self, destruction: datetime, job: UWSJob
    ) -> datetime:
        """Validate a new destruction time for a job.

        By default, changes to the job destruction time aren't allowed.

        Parameters
        ----------
        destruction
           New date at which the job outputs and its metadata will be deleted
           to recover resources.
        job
            Existing job.

        Returns
        -------
        datetime.datetime
            New destruction time for the job, which should be
            ``job.destruction_time`` if the policy layer doesn't want to allow
            any change.
        """
        return job.destruction_time

    def validate_execution_duration(
        self, execution_duration: int, job: UWSJob
    ) -> int:
        """Validate a new execution duration for a job.

        By default, changes to the execution duration of a job aren't allowed.

        Parameters
        ----------
        execution_duration
            New desired maximum execution time for the job in wall clock
            seconds.
        job
            Existing job.

        Returns
        -------
        int
            New execution duration for the job, which should be
            ``job.execution_duration`` if the policy layer doesn't want to
            allow any change.
        """
        return job.execution_duration

    @abstractmethod
    def validate_params(self, params: list[UWSJobParameter]) -> None:
        """Validate parameters for a job.

        Parameters
        ----------
        params
            Parameters for the job.

        Raises
        ------
        vocutouts.uws.exceptions.ParameterError
            Raised if one of the parameters was invalid.
        """
