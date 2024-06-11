"""Service layer for a UWS service."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta

from safir.arq import JobMetadata
from safir.datetime import current_datetime

from .config import UWSConfig
from .exceptions import InvalidPhaseError, PermissionDeniedError
from .models import (
    ACTIVE_PHASES,
    Availability,
    ExecutionPhase,
    UWSJob,
    UWSJobDescription,
    UWSJobParameter,
)
from .policy import UWSPolicy
from .storage import JobStore

__all__ = ["JobService"]


class JobService:
    """Dispatch and track UWS jobs.

    The goal of this layer is to encapsulate the machinery of a service that
    dispatches jobs using arq, without making assumptions about what the jobs
    do or what outputs they may return.  Workers do not use this layer and
    instead talk directly to the `~vocutouts.uws.storage.WorkerJobStore`.

    Parameters
    ----------
    config
        UWS configuration.
    policy
        Policy layer for dispatching jobs and validating parameters,
        destruction times, and execution durations.
    storage
        Underlying storage for job metadata and result tracking.
    """

    def __init__(
        self,
        config: UWSConfig,
        policy: UWSPolicy,
        storage: JobStore,
    ) -> None:
        self._config = config
        self._policy = policy
        self._storage = storage

    async def availability(self) -> Availability:
        """Check whether the service is up.

        Used for ``/availability`` endpoints.  Currently this only checks the
        database.  Eventually it should push an end-to-end test through the
        job execution system.

        Returns
        -------
        vocutouts.uws.models.Availability
            Service availability information.
        """
        return await self._storage.availability()

    async def create(
        self,
        user: str,
        *,
        run_id: str | None = None,
        params: list[UWSJobParameter],
    ) -> UWSJob:
        """Create a pending job.

        This does not start execution of the job.  That must be done
        separately with `start`.

        Parameters
        ----------
        user
            User on behalf this operation is performed.
        run_id
            A client-supplied opaque identifier to record with the job.
        params
            The input parameters to the job.

        Returns
        -------
        vocutouts.uws.models.Job
            The internal representation of the newly-created job.
        """
        self._policy.validate_params(params)
        return await self._storage.add(
            owner=user,
            run_id=run_id,
            params=params,
            execution_duration=self._config.execution_duration,
            lifetime=self._config.lifetime,
        )

    async def delete(
        self,
        user: str,
        job_id: str,
    ) -> None:
        """Delete a job.

        The UWS standard says that deleting a job should stop the in-progress
        work, but Dramatiq doesn't provide a way to do that.  Settle for
        deleting the database entry, which will cause the task to throw away
        the results when it finishes.
        """
        job = await self._storage.get(job_id)
        if job.owner != user:
            raise PermissionDeniedError(f"Access to job {job_id} denied")
        return await self._storage.delete(job_id)

    async def get(
        self,
        user: str,
        job_id: str,
        *,
        wait_seconds: int | None = None,
        wait_phase: ExecutionPhase | None = None,
        wait_for_completion: bool = False,
    ) -> UWSJob:
        """Retrieve a job.

        This also supports long-polling, to implement UWS 1.1 blocking
        behavior, and waiting for completion, to use as a building block when
        constructing a sync API.

        Parameters
        ----------
        user
            User on behalf this operation is performed.
        job_id
            Identifier of the job.
        wait
            If given, wait up to this many seconds for the status to change
            before returning. -1 indicates waiting the maximum length of
            time. This is done by polling the database with exponential
            backoff.  This will only be honored if the phase is ``PENDING``,
            ``QUEUED``, or ``EXECUTING``.
        wait_phase
            If ``wait`` was given, the starting phase for waiting.  Returns
            immediately if the initial phase doesn't match this one.
        wait_for_completion
            If set to true, wait until the job completes (has a phase other
            than ``QUEUED`` or ``EXECUTING``). Only one of this or
            ``wait_phase`` should be given. Ignored if ``wait`` was not given.

        Returns
        -------
        vocutouts.uws.models.Job
            The corresponding job.

        Raises
        ------
        vocutouts.uws.exceptions.PermissionDeniedError
            If the job ID doesn't exist or is for a user other than the
            provided user.

        Notes
        -----
        ``wait`` and related parameters are relatively inefficient since they
        poll the database using exponential backoff (starting at a 0.1s delay
        and increasing by 1.5x).  There doesn't seem to be a better solution
        without the added complexity of Dramatiq result storage and complex
        use of the Dramatiq message bus.  This may need to be reconsidered if
        it becomes a performance bottleneck.
        """
        job = await self._storage.get(job_id)
        if job.owner != user:
            raise PermissionDeniedError(f"Access to job {job_id} denied")

        # If waiting for a status change was requested and is meaningful, do
        # so, capping the wait time at the configured maximum timeout.
        if wait_seconds and job.phase in ACTIVE_PHASES:
            if wait_seconds < 0:
                wait = self._config.wait_timeout
            else:
                wait = timedelta(seconds=wait_seconds)
                if wait > self._config.wait_timeout:
                    wait = self._config.wait_timeout
            if wait_for_completion:
                until_not = ACTIVE_PHASES
            else:
                until_not = {wait_phase} if wait_phase else {job.phase}
            job = await self._wait_for_job(job, until_not, wait)

        return job

    async def list_jobs(
        self,
        user: str,
        *,
        phases: list[ExecutionPhase] | None = None,
        after: datetime | None = None,
        count: int | None = None,
    ) -> list[UWSJobDescription]:
        """List the jobs for a particular user.

        Parameters
        ----------
        user
            Name of the user whose jobs to load.
        phases
            Limit the result to jobs in this list of possible execution
            phases.
        after
            Limit the result to jobs created after the given datetime.
        count
            Limit the results to the most recent count jobs.

        Returns
        -------
        list of vocutouts.uws.models.JobDescription
            List of job descriptions matching the search criteria.
        """
        return await self._storage.list_jobs(
            user, phases=phases, after=after, count=count
        )

    async def start(self, user: str, job_id: str, token: str) -> JobMetadata:
        """Start execution of a job.

        Parameters
        ----------
        user
            User on behalf of whom this operation is performed.
        job_id
            Identifier of the job to start.
        token
            Gafaelfawr token used to authenticate to services used by the
            backend on the user's behalf.

        Returns
        -------
        JobMetadata
            arq job metadata.

        Raises
        ------
        vocutouts.uws.exceptions.PermissionDeniedError
            If the job ID doesn't exist or is for a user other than the
            provided user.
        """
        job = await self._storage.get(job_id)
        if job.owner != user:
            raise PermissionDeniedError(f"Access to job {job_id} denied")
        if job.phase not in (ExecutionPhase.PENDING, ExecutionPhase.HELD):
            raise InvalidPhaseError("Cannot start job in phase {job.phase}")
        metadata = await self._policy.dispatch(job, token)
        await self._storage.mark_queued(job_id, metadata)
        return metadata

    async def update_destruction(
        self, user: str, job_id: str, destruction: datetime
    ) -> datetime | None:
        """Update the destruction time of a job.

        Parameters
        ----------
        user
            User on behalf of whom this operation is performed
        job_id
            Identifier of the job to update.
        destruction
            The new job destruction time.  This may be arbitrarily modified
            by the policy layer.

        Returns
        -------
        datetime.datetime or None
            The new destruction time of the job (possibly modified by the
            policy layer), or `None` if the destruction time of the job was
            not changed.

        Raises
        ------
        vocutouts.uws.exceptions.PermissionDeniedError
            If the job ID doesn't exist or is for a user other than the
            provided user.
        """
        job = await self._storage.get(job_id)
        if job.owner != user:
            raise PermissionDeniedError(f"Access to job {job_id} denied")
        destruction = self._policy.validate_destruction(destruction, job)
        if destruction == job.destruction_time:
            return None
        else:
            await self._storage.update_destruction(job_id, destruction)
            return destruction

    async def update_execution_duration(
        self, user: str, job_id: str, duration: int
    ) -> int | None:
        """Update the execution duration time of a job.

        Parameters
        ----------
        user
            User on behalf of whom this operation is performed
        job_id
            Identifier of the job to update.
        duration
            The new job execution duration.  This may be arbitrarily modified
            by the policy layer.

        Returns
        -------
        int or None
            The new execution duration of the job (possibly modified by the
            policy layer), or `None` if the execution duration of the job was
            not changed.

        Raises
        ------
        vocutouts.uws.exceptions.PermissionDeniedError
            If the job ID doesn't exist or is for a user other than the
            provided user.
        """
        job = await self._storage.get(job_id)
        if job.owner != user:
            raise PermissionDeniedError(f"Access to job {job_id} denied")
        duration = self._policy.validate_execution_duration(duration, job)
        if duration == job.execution_duration:
            return None
        else:
            await self._storage.update_execution_duration(job_id, duration)
            return duration

    async def _wait_for_job(
        self, job: UWSJob, until_not: set[ExecutionPhase], timeout: timedelta
    ) -> UWSJob:
        """Wait for the completion of a job.

        Parameters
        ----------
        job
            Job to wait for.
        until_not
            Wait until the job is no longer in one of this set of phases.
        timeout
            How long to wait.

        Returns
        -------
        Job
            The new state of the job.
        """
        end_time = current_datetime(microseconds=True) + timeout
        now = current_datetime(microseconds=True)

        # I don't know of a way to set a watch on the database, so use
        # polling. Poll the database with exponential delay starting with 0.1
        # seconds and increasing by 1.5x each time until we reach the maximum
        # duration.
        delay = 0.1
        while job.phase in until_not and now < end_time:
            await asyncio.sleep(delay)
            job = await self._storage.get(job.job_id)
            now = current_datetime(microseconds=True)
            delay *= 1.5
            if now + timedelta(seconds=delay) > end_time:
                delay = (end_time - now).total_seconds()
        return job
