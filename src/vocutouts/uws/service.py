"""Service layer for a UWS service."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from .exceptions import InvalidPhaseError, PermissionDeniedError
from .models import ACTIVE_PHASES, ExecutionPhase

if TYPE_CHECKING:
    from typing import List, Optional

    from dramatiq import Actor, Message

    from .config import UWSConfig
    from .models import Availability, Job, JobDescription, JobParameter
    from .policy import UWSPolicy
    from .storage import FrontendJobStore

__all__ = ["JobService"]


class JobService:
    """Dispatch and track UWS jobs.

    The goal of this layer is to encapsulate the machinery of a service that
    dispatches jobs using Dramatiq, without making assumptions about what the
    jobs do or what outputs they may return.

    Parameters
    ----------
    config : `vocutouts.uws.config.UWSConfig`
        The UWS configuration.
    actor : `dramatiq.Actor`
        The actor to invoke to execute a job.
    policy : `vocutouts.uws.policy.UWSPolicy`
        The policy layer for validating parameters, destruction times, and
        execution durations.
    storage : `vocutouts.uws.storage.JobStore`
        The underlying storage for job metadata and result tracking.
    """

    def __init__(
        self,
        *,
        config: UWSConfig,
        actor: Actor,
        policy: UWSPolicy,
        storage: FrontendJobStore,
    ) -> None:
        self._config = config
        self._actor = actor
        self._policy = policy
        self._storage = storage

    async def availability(self) -> Availability:
        """Check whether the service is up.

        Used for ``/availability`` endpoints.  Currently this only checks the
        database.  Eventually it should push an end-to-end test through the
        job execution system.

        Returns
        -------
        available : `vocutouts.uws.models.Availability`
            Service availability information.
        """
        return await self._storage.availability()

    async def create(
        self,
        user: str,
        *,
        run_id: Optional[str] = None,
        params: List[JobParameter],
    ) -> Job:
        """Create a pending job.

        This does not start execution of the job.  That must be done
        separately with `queue`.

        Parameters
        ----------
        user : `str`
            User on behalf this operation is performed.
        run_id : `str`, optional
            A client-supplied opaque identifier to record with the job.
        params : List[`vocutouts.uws.models.JobParameter`]
            The input parameters to the job.

        Returns
        -------
        job : `vocutouts.uws.models.Job`
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

        Parameters
        ----------
        user : `str`
            User on behalf this operation is performed.
        job_id : `str`
            Identifier of the job.
        wait : `int`, optional
            If given, wait up to this many seconds for the status to change
            before returning.  ``-1`` says to wait the maximum length of time.
            This is done by polling the database with exponential backoff.
            This will only be honored if the phase is ``PENDING``, ``QUEUED``,
            or ``EXECUTING``.
        wait_phase : `vocutouts.uws.models.ExecutionPhase`, optional
            If ``wait`` was given, the starting phase for waiting.  Returns
            immediately if the initial phase doesn't match this one.
        wait_for_completion : `bool`, optional
            If set to true, wait until the job completes (has a phase other
            than ``QUEUED`` or ``EXECUTING``).

        Returns
        -------
        job : `vocutouts.uws.models.Job`
            The corresponding job.

        Raises
        ------
        vocutouts.uws.exceptions.PermissionDeniedError
            If the job ID doesn't exist or is for a user other than the
            provided user.
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
        phases: Optional[List[ExecutionPhase]] = None,
        after: Optional[datetime] = None,
        count: Optional[int] = None,
    ) -> List[JobDescription]:
        """List the jobs for a particular user.

        Parameters
        ----------
        user : `str`
            Name of the user whose jobs to load.
        phases : List[`vocutouts.uws.models.ExecutionPhase`], optional
            Limit the result to jobs in this list of possible execution
            phases.
        after : `datetime.datetime`, optional
            Limit the result to jobs created after the given datetime.
        count : `int`, optional
            Limit the results to the most recent count jobs.

        Returns
        -------
        descriptions : List[`vocutouts.uws.models.JobDescription`]
            List of job descriptions matching the search criteria.
        """
        return await self._storage.list_jobs(
            user, phases=phases, after=after, count=count
        )

    async def start(self, user: str, job_id: str) -> Message:
        """Start execution of a job.

        Parameters
        ----------
        user : `str`
            User on behalf of whom this operation is performed.
        job_id : `str`
            Identifier of the job to start.

        Returns
        -------
        message : `dramatiq.Message`
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
        message = self._actor.send_with_options(
            args=(job_id,), time_limit=job.execution_duration * 1000
        )
        await self._storage.mark_queued(job_id, message.message_id)
        return message

    async def update_destruction(
        self, user: str, job_id: str, destruction: datetime
    ) -> None:
        """Update the destruction time of a job.

        The caller must have already verified that the job is owned by the
        calling user (by, for instance, calling `get`).

        Parameters
        ----------
        user : `str`
            User on behalf of whom this operation is performed
        job_id : `str`
            Identifier of the job to update.
        destruction : `datetime.datetime`
            The new job destruction time.

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
        if destruction != job.destruction_time:
            await self._storage.update_destruction(job_id, destruction)

    async def update_execution_duration(
        self, user: str, job_id: str, duration: int
    ) -> None:
        """Update the execution duration time of a job.

        The caller must have already verified that the job is owned by the
        calling user (by, for instance, calling `get`).

        Parameters
        ----------
        user : `str`
            User on behalf of whom this operation is performed
        job_id : `str`
            Identifier of the job to update.
        duration : `int`
            The new job execution duration.

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
        if duration != job.execution_duration:
            await self._storage.update_execution_duration(job_id, duration)
