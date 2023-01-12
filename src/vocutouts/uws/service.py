"""Service layer for a UWS service."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional

from dramatiq import Message

from .config import UWSConfig
from .exceptions import InvalidPhaseError, PermissionDeniedError
from .models import (
    ACTIVE_PHASES,
    Availability,
    ExecutionPhase,
    Job,
    JobDescription,
    JobParameter,
)
from .policy import UWSPolicy
from .storage import FrontendJobStore

__all__ = ["JobService"]


class JobService:
    """Dispatch and track UWS jobs.

    The goal of this layer is to encapsulate the machinery of a service that
    dispatches jobs using Dramatiq, without making assumptions about what the
    jobs do or what outputs they may return.  Workers do not use this layer
    and instead talk directly to the `~vocutouts.uws.storage.WorkerJobStore`.

    Parameters
    ----------
    config
        The UWS configuration.
    policy
        The policy layer for dispatching jobs and validating parameters,
        destruction times, and execution durations.
    storage
        The underlying storage for job metadata and result tracking.
    """

    def __init__(
        self,
        *,
        config: UWSConfig,
        policy: UWSPolicy,
        storage: FrontendJobStore,
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
        run_id: Optional[str] = None,
        params: list[JobParameter],
    ) -> Job:
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
        wait: Optional[int] = None,
        wait_phase: Optional[ExecutionPhase] = None,
        wait_for_completion: bool = False,
    ) -> Job:
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
            before returning.  ``-1`` says to wait the maximum length of time.
            This is done by polling the database with exponential backoff.
            This will only be honored if the phase is ``PENDING``, ``QUEUED``,
            or ``EXECUTING``.
        wait_phase
            If ``wait`` was given, the starting phase for waiting.  Returns
            immediately if the initial phase doesn't match this one.
        wait_for_completion
            If set to true, wait until the job completes (has a phase other
            than ``QUEUED`` or ``EXECUTING``).  Only one of this or
            ``wait_phase`` should be given.  Ignored if ``wait`` was not
            given.

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
        if wait and job.phase in ACTIVE_PHASES:
            if wait < 0 or wait > self._config.wait_timeout:
                wait = self._config.wait_timeout
            end_time = datetime.now(tz=timezone.utc) + timedelta(seconds=wait)
            if not wait_phase:
                wait_phase = job.phase

            # Determine the criteria to stop waiting.
            def not_done(j: Job) -> bool:
                if wait_for_completion:
                    return j.phase in ACTIVE_PHASES
                else:
                    return j.phase == wait_phase

            # Poll the database with exponential delay starting with 0.1
            # seconds and increasing by 1.5x each time until we reach the
            # maximum duration.
            delay = 0.1
            while not_done(job):
                await asyncio.sleep(delay)
                job = await self._storage.get(job_id)
                now = datetime.now(tz=timezone.utc)
                if now >= end_time:
                    break
                delay *= 1.5
                if now + timedelta(seconds=delay) > end_time:
                    delay = (end_time - now).total_seconds()

        return job

    async def list_jobs(
        self,
        user: str,
        *,
        phases: Optional[list[ExecutionPhase]] = None,
        after: Optional[datetime] = None,
        count: Optional[int] = None,
    ) -> list[JobDescription]:
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

    async def start(self, user: str, job_id: str) -> Message:
        """Start execution of a job.

        Parameters
        ----------
        user
            User on behalf of whom this operation is performed.
        job_id
            Identifier of the job to start.

        Returns
        -------
        dramatiq.Message
            The work queuing message representing this job.

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
        message = self._policy.dispatch(job)
        await self._storage.mark_queued(job_id, message.message_id)
        return message

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
