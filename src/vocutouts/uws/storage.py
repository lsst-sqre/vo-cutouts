"""Storage layer for the UWS implementation."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone
from functools import wraps
from typing import Any, Optional, TypeVar, cast

from safir.database import datetime_from_db, datetime_to_db
from sqlalchemy import delete
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.ext.asyncio import async_scoped_session
from sqlalchemy.future import select
from sqlalchemy.orm import scoped_session

from .exceptions import UnknownJobError
from .models import (
    Availability,
    ExecutionPhase,
    Job,
    JobDescription,
    JobError,
    JobParameter,
    JobResult,
)
from .schema.job import Job as SQLJob
from .schema.job_parameter import JobParameter as SQLJobParameter
from .schema.job_result import JobResult as SQLJobResult

F = TypeVar("F", bound=Callable[..., Any])
G = TypeVar("G", bound=Callable[..., Awaitable[Any]])

__all__ = ["FrontendJobStore", "WorkerJobStore"]


def _convert_job(job: SQLJob) -> Job:
    """Convert the SQL representation of a job to its dataclass.

    The internal representation of a job uses a dataclass that is kept
    intentionally separate from the database schema so that the conversion
    can be done explicitly and the rest of the code kept separate from
    SQLAlchemy database models.  This internal helper function converts
    from the database representation to the internal representation.
    """
    error = None
    if job.error_code and job.error_type and job.error_message:
        error = JobError(
            error_type=job.error_type,
            error_code=job.error_code,
            message=job.error_message,
            detail=job.error_detail,
        )
    return Job(
        job_id=str(job.id),
        message_id=job.message_id,
        owner=job.owner,
        phase=job.phase,
        run_id=job.run_id,
        creation_time=datetime_from_db(job.creation_time),
        start_time=datetime_from_db(job.start_time),
        end_time=datetime_from_db(job.end_time),
        destruction_time=datetime_from_db(job.destruction_time),
        execution_duration=job.execution_duration,
        quote=job.quote,
        parameters=[
            JobParameter(
                parameter_id=p.parameter, value=p.value, is_post=p.is_post
            )
            for p in sorted(job.parameters, key=lambda p: p.id)
        ],
        results=[
            JobResult(
                result_id=r.result_id,
                url=r.url,
                size=r.size,
                mime_type=r.mime_type,
            )
            for r in sorted(job.results, key=lambda r: r.sequence)
        ],
        error=error,
    )


def retry_async_transaction(g: G) -> G:
    """Retry once if a transaction failed.

    Notes
    -----
    The UWS database workers may be run out of order (the one indicating the
    job has started may be run after the one indicating the job has finished,
    for example), which means we need a ``REPEATABLE READ`` transaction
    isolation level so that we can check if a job status change has already
    been done and avoid setting a job in ``COMPLETED`` back to ``EXECUTING``.

    Unfortunately, that isolation level causes the underlying database to
    raise an exception on commit if we raced with another worker.  We
    therefore need to retry if a transaction failed with an exception.

    The only functions that can race for a given job are the frontend setting
    the job status to ``QUEUED``, the backend setting it to ``EXECUTING``, and
    the backend setting it to ``COMPLETED`` or ``ERROR``.  Priorities should
    force the second to always execute before the third, so we should only
    race with at most one other SQL transaction.  Therefore, retrying once
    should be sufficient.
    """

    @wraps(g)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        for _ in range(1, 5):
            try:
                return await g(*args, **kwargs)
            except (DBAPIError, OperationalError):
                continue
        return await g(*args, **kwargs)

    return cast(G, wrapper)


class FrontendJobStore:
    """Stores and manipulates jobs in the database for the frontend.

    This is the async storage layer used by the web service frontend.  Workers
    use the `WorkerJobStore`, which is synchronous.

    Parameters
    ----------
    session
        The underlying database session.

    Notes
    -----
    Timestamp handling deserves special comment.  By default, SQLAlchemy
    databases do not store timestamp information in database rows.  It's
    possible to use a variant data type to do so in PostgreSQL, but since
    all database times will be in UTC, there's no need to do so.

    psycopg2 silently discards the UTC timezone information when storing a
    datetime (and apparently silently adds it when retrieving one).  However,
    asyncpg does not do this, and attempts to store a timezone-aware datetime
    in a database column that is not defined as holding timezone information
    results in an error.

    Best practices for Python are to make every datetime normally seen in the
    program timezone-aware so that one is never bitten by unexpected timezone
    variations.  Therefore, the storage layer should only expose
    timezone-aware datetimes.

    This is done by stripping the timezone from datetimes when stored in the
    database (making the assumption that all datetimes will use UTC, which is
    maintained by the rest of the UWS layer), and adding the UTC timezone back
    to datetimes when retrieved from the database.
    """

    def __init__(self, session: async_scoped_session) -> None:
        self._session = session

    async def add(
        self,
        *,
        owner: str,
        run_id: Optional[str] = None,
        params: list[JobParameter],
        execution_duration: int,
        lifetime: int,
    ) -> Job:
        """Create a record of a new job.

        The job will be created in pending status.

        Parameters
        ----------
        owner
            The username of the owner of the job.
        run_id
            A client-supplied opaque identifier to record with the job.
        params
            The input parameters to the job.
        execution_duration
            The maximum length of time for which a job is allowed to run in
            seconds.
        lifetime
            The maximum lifetime of the job and its results, in seconds.
            After this time, any record of the job will be deleted.

        Returns
        -------
        vocutouts.uws.models.Job
            The internal representation of the newly-created job.
        """
        now = datetime.now(tz=timezone.utc).replace(microsecond=0)
        destruction_time = now + timedelta(seconds=lifetime)
        sql_params = [
            SQLJobParameter(
                parameter=p.parameter_id,
                value=p.value,
                is_post=p.is_post,
            )
            for p in params
        ]
        job = SQLJob(
            owner=owner,
            phase=ExecutionPhase.PENDING,
            run_id=run_id,
            creation_time=datetime_to_db(now),
            destruction_time=datetime_to_db(destruction_time),
            execution_duration=execution_duration,
            parameters=sql_params,
            results=[],
        )
        async with self._session.begin():
            self._session.add_all([job, *sql_params])
            await self._session.flush()
            return _convert_job(job)

    async def availability(self) -> Availability:
        """Check that the database is up."""
        try:
            async with self._session.begin():
                await self._session.execute(select(SQLJob.id).limit(1))
            return Availability(available=True)
        except OperationalError:
            note = "cannot query UWS job database"
            return Availability(available=False, note=note)
        except Exception as e:
            note = f"{type(e).__name__}: {str(e)}"
            return Availability(available=False, note=note)

    async def delete(self, job_id: str) -> None:
        """Delete a job by ID."""
        async with self._session.begin():
            stmt = delete(SQLJob).where(SQLJob.id == int(job_id))
            await self._session.execute(stmt)

    async def get(self, job_id: str) -> Job:
        """Retrieve a job by ID."""
        async with self._session.begin():
            job = await self._get_job(job_id)
            return _convert_job(job)

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
            Limit the result to jobs created after the given datetime in UTC.
        count
            Limit the results to the most recent count jobs.

        Returns
        -------
        list of vocutouts.uws.models.JobDescription
            List of job descriptions matching the search criteria.
        """
        stmt = select(
            SQLJob.id,
            SQLJob.owner,
            SQLJob.phase,
            SQLJob.run_id,
            SQLJob.creation_time,
        ).where(SQLJob.owner == user)
        if phases:
            stmt = stmt.where(SQLJob.phase.in_(phases))
        if after:
            stmt = stmt.where(SQLJob.creation_time > datetime_to_db(after))
        stmt = stmt.order_by(SQLJob.creation_time.desc())
        if count:
            stmt = stmt.limit(count)
        async with self._session.begin():
            jobs = await self._session.execute(stmt)
            return [
                JobDescription(
                    job_id=str(j.id),
                    owner=j.owner,
                    phase=j.phase,
                    run_id=j.run_id,
                    creation_time=j.creation_time,
                )
                for j in jobs.all()
            ]

    @retry_async_transaction
    async def mark_queued(self, job_id: str, message_id: str) -> None:
        """Mark a job as queued for processing.

        This is called by the web frontend after queuing the work.  However,
        the worker may have gotten there first and have already updated the
        phase to executing, in which case we should not set it back to
        queued.

        Parameters
        ----------
        job_id
            The identifier of the job.
        message_id
            The identifier for the execution of that job in the work queuing
            system.
        """
        async with self._session.begin():
            job = await self._get_job(job_id)
            job.message_id = message_id
            if job.phase in (ExecutionPhase.PENDING, ExecutionPhase.HELD):
                job.phase = ExecutionPhase.QUEUED

    async def update_destruction(
        self, job_id: str, destruction: datetime
    ) -> None:
        """Update the destruction time of a job.

        Parameters
        ----------
        job_id
            The identifier of the job.
        destruction
            The new destruction time.
        """
        async with self._session.begin():
            job = await self._get_job(job_id)
            job.destruction_time = datetime_to_db(destruction)

    async def update_execution_duration(
        self, job_id: str, execution_duration: int
    ) -> None:
        """Update the destruction time of a job.

        Parameters
        ----------
        job_id
            The identifier of the job.
        execution_duration
            The new execution duration.
        """
        async with self._session.begin():
            job = await self._get_job(job_id)
            job.execution_duration = execution_duration

    async def _get_job(self, job_id: str) -> SQLJob:
        """Retrieve a job from the database by job ID."""
        stmt = select(SQLJob).where(SQLJob.id == int(job_id))
        job = (await self._session.execute(stmt)).scalar_one_or_none()
        if not job:
            raise UnknownJobError(job_id)
        return job


def retry_transaction(f: F) -> F:
    """Retry once if a transaction failed.

    Notes
    -----
    The UWS database workers may be run out of order (the one indicating the
    job has started may be run after the one indicating the job has finished,
    for example), which means we need a ``REPEATABLE READ`` transaction
    isolation level so that we can check if a job status change has already
    been done and avoid setting a job in ``COMPLETED`` back to ``EXECUTING``.

    Unfortunately, that isolation level causes the underlying database to
    raise an exception on commit if we raced with another worker.  We
    therefore need to retry if a transaction failed with an exception.
    """

    @wraps(f)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        for _ in range(1, 5):
            try:
                return f(*args, **kwargs)
            except OperationalError:
                continue
        return f(*args, **kwargs)

    return cast(F, wrapper)


class WorkerJobStore:
    """Records worker actions in the database.

    This is the synchronous storage layer used by the backend workers.

    Parameters
    ----------
    session
        The underlying database session.
    """

    def __init__(self, session: scoped_session) -> None:
        self._session = session

    @retry_transaction
    def mark_completed(self, job_id: str, results: list[JobResult]) -> None:
        """Mark a job as completed."""
        with self._session.begin():
            job = self._get_job(job_id)
            job.phase = ExecutionPhase.COMPLETED
            job.end_time = datetime_to_db(datetime.now(tz=timezone.utc))
            for sequence, result in enumerate(results, start=1):
                sql_result = SQLJobResult(
                    job_id=job.id,
                    result_id=result.result_id,
                    sequence=sequence,
                    url=result.url,
                    size=result.size,
                    mime_type=result.mime_type,
                )
                self._session.add(sql_result)

    @retry_transaction
    def mark_errored(self, job_id: str, error: JobError) -> None:
        """Mark a job as failed with an error."""
        with self._session.begin():
            job = self._get_job(job_id)
            job.phase = ExecutionPhase.ERROR
            job.end_time = datetime_to_db(datetime.now(tz=timezone.utc))
            job.error_type = error.error_type
            job.error_code = error.error_code
            job.error_message = error.message
            job.error_detail = error.detail

    @retry_transaction
    def start_executing(
        self, job_id: str, message_id: str, start_time: datetime
    ) -> None:
        """Mark a job as executing.

        Parameters
        ----------
        job_id
            The identifier of the job.
        message_id
            The identifier for the execution of that job in the work queuing
            system.
        start_time
            The time at which the job started executing.
        """
        with self._session.begin():
            job = self._get_job(job_id)
            if job.phase in (ExecutionPhase.PENDING, ExecutionPhase.QUEUED):
                job.phase = ExecutionPhase.EXECUTING
            job.start_time = datetime_to_db(start_time)
            job.message_id = message_id

    def _get_job(self, job_id: str) -> SQLJob:
        """Retrieve a job from the database by job ID."""
        stmt = select(SQLJob).where(SQLJob.id == int(job_id))
        job = self._session.execute(stmt).scalar_one_or_none()
        if not job:
            raise UnknownJobError(job_id)
        return job
